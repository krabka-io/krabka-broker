//! Opt-in isolated Java TaskAssignor execution without blocking the coordinator.
//!
//! Register this adapter in `StreamsGroupConfig::task_assignors`. An assignment
//! not yet ready returns a normal assignor error: the actor retains its previous
//! target and the client retries. Only an exactly matching immutable input may
//! consume a completed result. Results from obsolete membership/topology inputs
//! are never installed into a new target. This is a pure-function bridge: plugins
//! must satisfy the native SPI's deterministic-input contract.
use {
    super::assignor::{AssignorInput, AssignorMember, StreamsAssignment, TaskAssignor},
    serde::Deserialize,
    serde_json::{Value, json},
    std::{
        collections::{BTreeMap, HashMap},
        path::PathBuf,
        process::Stdio,
        sync::Mutex,
        time::{Duration, Instant},
    },
    tokio::{
        io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
        process::Command,
        task::JoinHandle,
    },
};

const BYTE_LIMIT: usize = 1024 * 1024;
const JOB_LIMIT: usize = 32;
const CACHE_LIFETIME: Duration = Duration::from_secs(60);

/// Java launch settings. Classpath must include AssignorWorker, released Kafka
/// 4.4 libraries and the trusted plugin JAR. No shell interpretation is used.
#[derive(Clone, Debug)]
pub struct JvmAssignorOptions {
    pub java: PathBuf,
    pub classpath: String,
    pub class_name: String,
    pub timeout: Duration,
}

#[derive(Debug)]
struct Job {
    created: Instant,
    handle: Option<JoinHandle<Result<StreamsAssignment, String>>>,
    result: Option<Result<StreamsAssignment, String>>,
}
impl Drop for Job {
    fn drop(&mut self) {
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }
}

/// An async JVM job cache behind the nonblocking native assignor interface.
///
/// Bounded to 32 requests and 1 MiB per input/output stream. Expired or dropped
/// jobs abort their worker; Java children have kill-on-drop enabled. Concurrent
/// requests cannot wait on the cache mutex: contention returns an assignor error.
#[derive(Debug)]
pub struct JvmTaskAssignor {
    name: String,
    options: JvmAssignorOptions,
    jobs: Mutex<BTreeMap<Vec<u8>, Job>>,
}
impl JvmTaskAssignor {
    /// Construct an adapter. Its registration name can differ from Java's name.
    /// # Errors
    /// Rejects empty names/classes/classpaths and zero deadlines.
    pub fn new(name: String, options: JvmAssignorOptions) -> Result<Self, String> {
        if name.is_empty()
            || options.class_name.is_empty()
            || options.classpath.is_empty()
            || options.timeout.is_zero()
        {
            return Err("JVM assignor needs a name, classpath, class and nonzero timeout".into());
        }
        Ok(Self {
            name,
            options,
            jobs: Mutex::new(BTreeMap::new()),
        })
    }
}

fn request(members: &[AssignorMember], input: &AssignorInput) -> Result<Vec<u8>, String> {
    let mut ordered: Vec<_> = members.iter().collect();
    ordered.sort_by(|a, b| a.member_id.cmp(&b.member_id));
    let members: Vec<Value> = ordered.into_iter().map(|m| {
        let mut offsets = BTreeMap::<&str, BTreeMap<String, i64>>::new();
        for ((sub, partition), offset) in &m.task_offsets {
            offsets.entry(sub).or_default().insert(partition.to_string(), *offset);
        }
        json!({"member_id":m.member_id,"process_id":m.process_id,"current_active":m.current_active,
            "current_standby":m.current_standby,"current_warmup":m.current_warmup,"task_offsets":offsets})
    }).collect();
    let bytes = serde_json::to_vec(&json!({"version":1,"members":members,"input":{
        "tasks":input.tasks,"stateful":input.stateful,"num_standby_replicas":input.num_standby_replicas
    }})).map_err(|e| e.to_string())?;
    if bytes.len() > BYTE_LIMIT {
        return Err("JVM assignment input exceeds byte limit".into());
    }
    Ok(bytes)
}

impl TaskAssignor for JvmTaskAssignor {
    fn name(&self) -> &str { &self.name }

    fn assign(
        &self,
        members: &[AssignorMember],
        input: &AssignorInput,
    ) -> Result<StreamsAssignment, String> {
        let key = request(members, input)?;
        let mut jobs = self
            .jobs
            .try_lock()
            .map_err(|_| "JVM assignment cache busy".to_string())?;
        jobs.retain(|_, job| job.created.elapsed() < CACHE_LIFETIME);
        if let Some(job) = jobs.get_mut(&key) {
            if job.result.is_none() && job.handle.as_ref().is_some_and(JoinHandle::is_finished) {
                use futures_util::FutureExt;
                let handle = job.handle.take().expect("finished worker handle");
                job.result = Some(
                    handle
                        .now_or_never()
                        .expect("finished worker")
                        .unwrap_or_else(|e| Err(format!("JVM assignment task failed: {e}"))),
                );
            }
            return job
                .result
                .clone()
                .unwrap_or_else(|| Err("JVM assignment pending; retry heartbeat".into()));
        }
        if jobs.len() >= JOB_LIMIT {
            return Err("JVM assignor request capacity reached".into());
        }
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|e| format!("JVM assignor needs a Tokio runtime: {e}"))?;
        let options = self.options.clone();
        let payload = key.clone();
        let members = members.to_vec();
        let input = input.clone();
        let handle = runtime.spawn(async move {
            let result = tokio::time::timeout(options.timeout, execute(&options, payload))
                .await
                .map_err(|_| "JVM assignment deadline exceeded".to_string())??;
            super::assignor::validate_assignment(&result, &members, &input)?;
            Ok(result)
        });
        jobs.insert(key, Job {
            created: Instant::now(),
            handle: Some(handle),
            result: None,
        });
        Err("JVM assignment pending; retry heartbeat".into())
    }
}

async fn bounded_read<R: AsyncRead + Unpin>(stream: R) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    stream
        .take((BYTE_LIMIT + 1) as u64)
        .read_to_end(&mut bytes)
        .await
        .map_err(|e| e.to_string())?;
    if bytes.len() > BYTE_LIMIT {
        return Err("JVM worker output exceeds byte limit".into());
    }
    Ok(bytes)
}

#[derive(Deserialize)]
struct Response {
    version: i32,
    active: HashMap<String, BTreeMap<String, Vec<i32>>>,
    standby: HashMap<String, BTreeMap<String, Vec<i32>>>,
}
async fn execute(
    options: &JvmAssignorOptions,
    payload: Vec<u8>,
) -> Result<StreamsAssignment, String> {
    let mut child = Command::new(&options.java)
        .args([
            "-Xmx128m",
            "-cp",
            &options.classpath,
            "AssignorWorker",
            &options.class_name,
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| format!("launch JVM assignor: {e}"))?;
    let mut stdin = child.stdin.take().ok_or("JVM worker missing stdin")?;
    let stdout = child.stdout.take().ok_or("JVM worker missing stdout")?;
    let stderr = child.stderr.take().ok_or("JVM worker missing stderr")?;
    let write = async move {
        stdin.write_all(&payload).await.map_err(|e| e.to_string())?;
        stdin.shutdown().await.map_err(|e| e.to_string())
    };
    let wait = async { child.wait().await.map_err(|e| e.to_string()) };
    let (_, stdout, stderr, status) =
        tokio::try_join!(write, bounded_read(stdout), bounded_read(stderr), wait)?;
    if !status.success() {
        return Err(format!(
            "JVM assignor failed ({status}): {}",
            String::from_utf8_lossy(&stderr[stderr.len().saturating_sub(4096)..])
        ));
    }
    let response: Response = serde_json::from_slice(&stdout)
        .map_err(|e| format!("invalid JVM assignment response: {e}"))?;
    if response.version != 1 {
        return Err("unsupported JVM assignment response version".into());
    }
    Ok(StreamsAssignment {
        active: response.active,
        standby: response.standby,
    })
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use {
        super::*,
        std::{fs, os::unix::fs::PermissionsExt},
        tempfile::TempDir,
    };

    fn hanging_worker() -> (TempDir, JvmTaskAssignor) {
        let test_root =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/jvm-assignor-tests");
        fs::create_dir_all(&test_root).unwrap();
        let dir = tempfile::tempdir_in(test_root).unwrap();
        let script = dir.path().join("java");
        let pid_file = dir.path().join("pid");
        fs::write(&script, format!("#!/usr/bin/env python3\nimport os,time\nopen({:?},'w').write(str(os.getpid()))\nwhile True: time.sleep(1)\n", pid_file.to_str().unwrap())).unwrap();
        fs::set_permissions(&script, fs::Permissions::from_mode(0o700)).unwrap();
        let adapter = JvmTaskAssignor::new("test-jvm".into(), JvmAssignorOptions {
            java: script,
            classpath: "ignored".into(),
            class_name: "ignored".into(),
            timeout: Duration::from_millis(250),
        })
        .unwrap();
        (dir, adapter)
    }

    async fn worker_pid(dir: &TempDir) -> u32 {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(pid) = fs::read_to_string(dir.path().join("pid"))
                    && let Ok(pid) = pid.parse()
                {
                    break pid;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("worker started")
    }

    async fn reaped(pid: u32) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while PathBuf::from(format!("/proc/{pid}")).exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("worker killed and reaped")
    }

    #[tokio::test]
    async fn jvm_assignor_deadline_kills_and_reaps_worker() {
        let (dir, adapter) = hanging_worker();
        let members = vec![AssignorMember {
            member_id: "a".into(),
            ..Default::default()
        }];
        let input = AssignorInput::default();
        assert!(
            adapter
                .assign(&members, &input)
                .unwrap_err()
                .contains("pending")
        );
        let pid = worker_pid(&dir).await;
        reaped(pid).await;
        assert!(
            adapter
                .assign(&members, &input)
                .unwrap_err()
                .contains("deadline")
        );
    }

    #[tokio::test]
    async fn jvm_assignor_drop_cancels_and_reaps_worker() {
        let (dir, adapter) = hanging_worker();
        let members = vec![AssignorMember {
            member_id: "a".into(),
            ..Default::default()
        }];
        assert!(adapter.assign(&members, &AssignorInput::default()).is_err());
        let pid = worker_pid(&dir).await;
        drop(adapter);
        reaped(pid).await;
    }

    #[test]
    fn jvm_assignor_input_fence_includes_membership_state_topology_and_config() {
        let mut members = vec![
            AssignorMember {
                member_id: "a".into(),
                ..Default::default()
            },
            AssignorMember {
                member_id: "b".into(),
                ..Default::default()
            },
        ];
        let mut input = AssignorInput::default();
        let original = request(&members, &input).unwrap();
        members.reverse();
        assert_eq!(original, request(&members, &input).unwrap());
        members[0].task_offsets.insert(("s".into(), 0), i64::MAX);
        assert_ne!(original, request(&members, &input).unwrap());
        members[0].task_offsets.clear();
        input.num_standby_replicas = 1;
        assert_ne!(original, request(&members, &input).unwrap());
        input.num_standby_replicas = 0;
        input.tasks.insert("s".into(), vec![0]);
        assert_ne!(original, request(&members, &input).unwrap());
        input.tasks.clear();
        members.pop();
        assert_ne!(original, request(&members, &input).unwrap());
    }
}
