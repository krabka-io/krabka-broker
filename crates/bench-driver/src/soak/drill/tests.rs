use {
    super::{Objectives, PathShape, load_contract, objectives, read_load, write_json},
    crate::soak::{
        contract::{Contract, fixture},
        evidence::Evidence,
        recovery::{Ack, CommitAck, CommitHistory, LoadRecord, ProducerAcks},
    },
    assert2::{assert, check},
    std::path::Path,
};

fn write_contract(dir: &Path, contract: &Contract) -> std::path::PathBuf {
    let path = dir.join(format!("{}.yaml", contract.name));
    std::fs::write(&path, serde_saphyr::to_string(contract).unwrap()).unwrap();
    path
}

#[test]
fn a_drill_contract_declares_its_objectives_and_topics_in_the_units_the_drill_reads() {
    let shape = |name: &str, storage: &str| PathShape {
        name: name.to_owned(),
        storage: storage.to_owned(),
        partitions: 3,
        min_insync_replicas: 2,
    };
    assert!(
        objectives(&fixture::drill()).unwrap()
            == Objectives {
                post_capture_load_ms: 20_000,
                rpo_ms: 120_000,
                rto_ms: 600_000,
                paths: vec![
                    shape("tiered-idempotent", "tiered"),
                    shape("diskless-idempotent", "diskless"),
                ],
            }
    );
}

#[test]
fn a_contract_without_recovery_objectives_has_none_to_print() {
    let error = objectives(&fixture::self_check()).unwrap_err().to_string();
    check!(error.contains("declares its objectives"), "{error}");
}

#[test]
fn only_a_disaster_recovery_contract_loads_as_a_drill() {
    let dir = tempfile::tempdir().unwrap();
    let drill = fixture::drill();
    check!(load_contract(&write_contract(dir.path(), &drill)).unwrap() == drill);

    let cases = [
        (
            write_contract(dir.path(), &fixture::self_check()),
            "the drill runs only disaster_recovery contracts",
        ),
        (dir.path().join("absent.yaml"), "read "),
    ];
    for (path, expected) in cases {
        let error = load_contract(&path).unwrap_err().to_string();
        check!(error.contains(expected), "{}: {error}", path.display());
    }
}

#[test]
fn a_load_record_reads_back_as_it_was_written() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("load.json");
    let record = LoadRecord {
        run: Evidence {
            contract: "fixture-drill".to_owned(),
            started_unix_ms: 1_000,
            ended_unix_ms: 9_000,
            ..Evidence::default()
        },
        acks: vec![ProducerAcks {
            producer: "tiered-idempotent-0".to_owned(),
            acks: vec![
                Ack {
                    seq: 0,
                    unix_ms: 1_200,
                },
                Ack {
                    seq: 1,
                    unix_ms: 1_300,
                },
            ],
        }],
        commits: vec![CommitHistory {
            group: "soak-tiered-idempotent".to_owned(),
            topic: "tiered-idempotent".to_owned(),
            partitions: 3,
            acks: vec![CommitAck {
                unix_ms: 2_000,
                offsets: [(0, 4), (2, 1)].into_iter().collect(),
            }],
        }],
    };

    write_json(&path, &record).unwrap();

    assert!(read_load(&path).unwrap() == record);
    check!(!path.with_extension("partial").exists());
}

#[test]
fn a_load_record_that_is_not_one_is_refused_by_name() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("load.json");
    std::fs::write(&path, "{\"run\": 7}").unwrap();
    let error = read_load(&path).unwrap_err().to_string();
    check!(error.contains("parse load record"), "{error}");
}
