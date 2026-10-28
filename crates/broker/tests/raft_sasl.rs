//! Inbound raft listener auth tests.
//!
//! These tests exercise the controller listener under `SaslPlaintext`.
//! They prove that the inbound and the outbound path work together. On
//! the inbound path, broker A accepts auth'd raft frames from broker B.
//! On the outbound path, `InterBrokerDialer` dials with SASL credentials.
#![cfg(test)]

mod support;

use {
    assert2::assert,
    base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD},
    krabka_broker::{
        BootstrapMode, Broker, BrokerConfig, BrokerHandle, authorizer::SimpleAclAuthorizer,
        config::InterBrokerCredentials,
    },
    krabka_raft::NodeId,
    krabka_security::{ListenerProtocol, SaslMechanism},
    std::{collections::HashSet, fs, net::SocketAddr, path::Path, sync::Arc, time::Duration},
    tempfile::TempDir,
    tokio::{net::TcpListener, time},
};

fn oauth_token() -> String {
    format!(
        "{}.{}.",
        URL_SAFE_NO_PAD.encode(br#"{"alg":"none"}"#),
        URL_SAFE_NO_PAD.encode(br#"{"sub":"broker","exp":4000000000}"#)
    )
}

use crate::support::{
    NodeIndex, controller_voters, init_tracing, listeners::configure_sasl_plaintext, node_config,
};

#[derive(derive_more::From, derive_more::Into, Clone, Copy, Default)]
struct BrokerSlot(usize);

#[derive(Clone, Copy, PartialEq, Eq)]
enum ControllerAuthorization {
    Allowed,
    Denied,
}

/// The protocol a controller listener speaks and the address it binds.
#[derive(Clone, Copy)]
struct ControllerListener {
    protocol: ListenerProtocol,
    addr: SocketAddr,
}

/// One member of the static controller voter set and its controller address.
#[derive(Clone, Copy)]
struct ControllerVoter {
    id: NodeId,
    addr: SocketAddr,
}

/// The SASL/PLAIN user name and password a broker presents and accepts.
#[derive(Clone, Copy)]
struct PlainCredentials<'a> {
    user: &'a str,
    password: &'a str,
}

/// Build a `SASL_PLAINTEXT` data-plane listener config for broker `i`
/// (0-indexed) and parameterized `controller_listener_protocol`.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct SaslBrokerSetup<'a> {
    slot: BrokerSlot,
    #[default(data_listen_addr())]
    data_addr: SocketAddr,
    #[default(ControllerListener {
        protocol: ListenerProtocol::SaslPlaintext,
        addr: data_listen_addr(),
    })]
    controller: ControllerListener,
    voters: &'a [ControllerVoter],
    #[default(BootstrapMode::Bootstrap)]
    mode: BootstrapMode,
    #[default(PlainCredentials {
        user: "raft-user",
        password: "raft-password",
    })]
    credentials: PlainCredentials<'a>,
}

#[bon::builder]
fn sasl_broker_config(
    log_dir: &Path,
    #[builder(into, default)] slot: BrokerSlot,
    #[builder(default = data_listen_addr ())] data_addr: SocketAddr,
    #[builder(default = ControllerListener { protocol : ListenerProtocol :: SaslPlaintext , addr : data_listen_addr () })]
    controller: ControllerListener,
    #[builder(default)] voters: &'_ [ControllerVoter],
    #[builder(default = BootstrapMode :: Bootstrap)] mode: BootstrapMode,
    #[builder(default = PlainCredentials { user : "raft-user" , password : "raft-password" })]
    credentials: PlainCredentials<'_>,
) -> BrokerConfig {
    let setup = SaslBrokerSetup {
        slot,
        data_addr,
        controller,
        voters,
        mode,
        credentials,
    };
    let SaslBrokerSetup {
        slot,
        data_addr,
        controller,
        voters,
        mode,
        credentials,
    } = setup;
    let ControllerListener {
        protocol: ctrl,
        addr: ctrl_addr,
    } = controller;
    let PlainCredentials {
        user: plain_user,
        password: plain_pass,
    } = credentials;
    let mut cfg = node_config(NodeIndex(slot.0), log_dir);
    cfg.listen_addr = data_addr;
    cfg.advertised_listener = data_addr.to_string();
    cfg.controller_listen_addr = ctrl_addr;
    cfg.controller_quorum_voters = voters
        .iter()
        .map(|ControllerVoter { id, addr }| (*id, addr.to_string()))
        .collect();
    cfg.bootstrap_mode = mode;
    configure_sasl_plaintext(&mut cfg, data_addr, ctrl);
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    cfg.plain_credentials
        .insert(plain_user.to_owned(), plain_pass.to_owned());
    cfg.inter_broker_credentials = Some(InterBrokerCredentials::Plain {
        username: plain_user.to_owned(),
        password: plain_pass.to_owned(),
    });
    cfg
}

/// Two bound controller listeners and the addresses they are bound to.
struct ReservedControllers {
    addrs: [SocketAddr; 2],
    listeners: [TcpListener; 2],
}

/// Bind two ephemeral loopback controller listeners and return them
/// alongside their addresses. The caller hands the live listeners to
/// `Broker::start_with_controller_listener`, which adopts them directly
/// instead of re-binding the address.
///
/// This defeats the bind-and-drop TOCTOU race. The classic pattern reads
/// an ephemeral port, then *drops* the probe socket before the broker
/// re-binds it. That leaves a window in which another process on the
/// runner can claim the port, and `Broker::start` then fails with
/// `AddrInUse`. This helper keeps the socket bound and hands it over,
/// which removes that window.
async fn reserve_ctrl_listeners() -> ReservedControllers {
    let l0 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let l1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a0 = l0.local_addr().unwrap();
    let a1 = l1.local_addr().unwrap();
    ReservedControllers {
        addrs: [a0, a1],
        listeners: [l0, l1],
    }
}

/// Data-plane bind address for these tests: `127.0.0.1:0` lets the OS
/// assign an ephemeral port at `Broker::start`, so there's no probe/drop
/// gap to race on. Convergence here uses the controller listener. No
/// test dials the data plane, so nothing reads the bound port back.
fn data_listen_addr() -> SocketAddr { "127.0.0.1:0".parse().unwrap() }

/// Two running brokers and the data directory behind each.
struct TwoBrokers {
    broker0: BrokerHandle,
    broker1: BrokerHandle,
    dir0: TempDir,
    dir1: TempDir,
}

/// Boot two brokers with the supplied data-plane and controller listener
/// configurations. This helper uses the deterministic bootstrap-then-join
/// pattern. The two "converging" tests call it. The mismatched-creds test
/// below inlines its own setup because it must spawn the joiner
/// asynchronously. That test never gets a leader.
async fn start_two_brokers_with_controller_protocol(
    ctrl: ListenerProtocol,
    plain_user: &str,
    plain_pass: &str,
) -> TwoBrokers {
    init_tracing();
    let ReservedControllers {
        addrs: ctrl_addrs,
        listeners: [ctrl_l0, ctrl_l1],
    } = reserve_ctrl_listeners().await;
    let voters = [
        ControllerVoter {
            id: NodeId(1),
            addr: ctrl_addrs[0],
        },
        ControllerVoter {
            id: NodeId(2),
            addr: ctrl_addrs[1],
        },
    ];

    let dir0 = TempDir::new().unwrap();
    let dir1 = TempDir::new().unwrap();

    let cfg0 = sasl_broker_config()
        .log_dir(dir0.path())
        .controller(ControllerListener {
            protocol: ctrl,
            addr: ctrl_addrs[0],
        })
        .voters(&voters)
        .credentials(PlainCredentials {
            user: plain_user,
            password: plain_pass,
        })
        .call();
    let cfg1 = sasl_broker_config()
        .log_dir(dir1.path())
        .slot(1)
        .controller(ControllerListener {
            protocol: ctrl,
            addr: ctrl_addrs[1],
        })
        .voters(&voters)
        .credentials(PlainCredentials {
            user: plain_user,
            password: plain_pass,
        })
        .call();

    // KIP-595 static-quorum bootstrap: both brokers boot with the same
    // static voter set and elect among themselves over the (SASL/plaintext)
    // controller wire — no add_learner / change_membership (KIP-853 dynamic voter reconfiguration).
    let cfg1_for_spawn = cfg1.clone();
    let join = tokio::spawn(async move {
        Broker::start_with_controller_listener(cfg1_for_spawn, Some(ctrl_l1)).await
    });
    let broker0 = Broker::start_with_controller_listener(cfg0, Some(ctrl_l0))
        .await
        .expect("start broker 0");

    let broker1 = join.await.expect("join spawn").expect("start broker 1");
    TwoBrokers {
        broker0,
        broker1,
        dir0,
        dir1,
    }
}

async fn start_independent_controllers(
    credentials: [PlainCredentials<'_>; 2],
    authorization: ControllerAuthorization,
) -> TwoBrokers {
    init_tracing();
    let ReservedControllers {
        addrs: ctrl_addrs,
        listeners: [ctrl_l1, ctrl_l2],
    } = reserve_ctrl_listeners().await;
    let dir1 = TempDir::new().unwrap();
    let dir2 = TempDir::new().unwrap();
    // A shared two-voter quorum would never elect with these failures and would
    // block Broker::start on its leader wait. Each node instead bootstraps itself.
    let config = |index: usize, dir: &TempDir| {
        let mut config = sasl_broker_config()
            .log_dir(dir.path())
            .slot(index)
            .controller(ControllerListener {
                protocol: ListenerProtocol::SaslPlaintext,
                addr: ctrl_addrs[index],
            })
            .voters(&[ControllerVoter {
                id: NodeId(u64::try_from(index).unwrap() + 1),
                addr: ctrl_addrs[index],
            }])
            .credentials(credentials[index])
            .call();
        if authorization == ControllerAuthorization::Denied {
            // Valid SASL credentials are still denied CLUSTER_ACTION without
            // super users or ACLs. Construct each authorizer independently.
            config.authorizer = Arc::new(SimpleAclAuthorizer::new(HashSet::new()));
        }
        config
    };
    let c1 = config(0, &dir1);
    let c2 = config(1, &dir2);
    let b1 = Broker::start_with_controller_listener(c1, Some(ctrl_l1))
        .await
        .expect("start b1");
    let b2 = Broker::start_with_controller_listener(c2, Some(ctrl_l2))
        .await
        .expect("start b2");
    TwoBrokers {
        broker0: b1,
        broker1: b2,
        dir0: dir1,
        dir1: dir2,
    }
}

async fn start_oauth_controllers() -> (TwoBrokers, TempDir) {
    init_tracing();
    let ReservedControllers {
        addrs: controller_addrs,
        listeners: [controller_0, controller_1],
    } = reserve_ctrl_listeners().await;
    let voters = vec![
        ControllerVoter {
            id: NodeId(1),
            addr: controller_addrs[0],
        },
        ControllerVoter {
            id: NodeId(2),
            addr: controller_addrs[1],
        },
    ];
    let dir0 = TempDir::new().unwrap();
    let dir1 = TempDir::new().unwrap();
    let token_dir = TempDir::new().unwrap();
    let token_path = token_dir.path().join("oauth-token");
    fs::write(&token_path, oauth_token()).unwrap();

    let mut cfg0 = sasl_broker_config()
        .log_dir(dir0.path())
        .controller(ControllerListener {
            protocol: ListenerProtocol::SaslPlaintext,
            addr: controller_addrs[0],
        })
        .voters(&voters)
        .credentials(PlainCredentials {
            user: "unused",
            password: "unused",
        })
        .call();
    let mut cfg1 = sasl_broker_config()
        .log_dir(dir1.path())
        .slot(1)
        .controller(ControllerListener {
            protocol: ListenerProtocol::SaslPlaintext,
            addr: controller_addrs[1],
        })
        .voters(&voters)
        .credentials(PlainCredentials {
            user: "unused",
            password: "unused",
        })
        .call();
    for config in [&mut cfg0, &mut cfg1] {
        config.enabled_sasl_mechanisms = vec![SaslMechanism::OAuthBearer];
        config.plain_credentials.clear();
        config.inter_broker_credentials = Some(InterBrokerCredentials::OAuthBearer {
            token_path: token_path.clone(),
        });
    }

    let join0 = tokio::spawn(async move {
        Broker::start_with_controller_listener(cfg0, Some(controller_0)).await
    });
    let join1 = tokio::spawn(async move {
        Broker::start_with_controller_listener(cfg1, Some(controller_1)).await
    });
    let broker0 = join0.await.expect("broker 0 task").expect("start broker 0");
    let broker1 = join1.await.expect("broker 1 task").expect("start broker 1");

    (
        TwoBrokers {
            broker0,
            broker1,
            dir0,
            dir1,
        },
        token_dir,
    )
}

async fn start_legacy_controllers() -> TwoBrokers {
    // Default `controller_listener_protocol = Plaintext` and the default
    // `AllowAllAuthorizer`: every peer is `ANONYMOUS` and every request is
    // allowed. Two brokers converge over the plaintext path.
    init_tracing();
    let ReservedControllers {
        addrs: ctrl_addrs,
        listeners: [ctrl_l1, ctrl_l2],
    } = reserve_ctrl_listeners().await;
    let voters = vec![(1, ctrl_addrs[0]), (2, ctrl_addrs[1])];

    let dir1 = TempDir::new().unwrap();
    let dir2 = TempDir::new().unwrap();

    // Plain (no SASL) configs: don't use sasl_broker_config because we
    // want zero auth on either listener (legacy path).
    let mut c1 = node_config(NodeIndex(0), dir1.path());
    c1.listen_addr = data_listen_addr();
    c1.advertised_listener = data_listen_addr().to_string();
    c1.controller_listen_addr = ctrl_addrs[0];
    c1.controller_quorum_voters = controller_voters(&voters);
    c1.bootstrap_mode = BootstrapMode::Bootstrap;
    c1.controller_listener_protocol = ListenerProtocol::Plaintext;

    let mut c2 = node_config(NodeIndex(1), dir2.path());
    c2.listen_addr = data_listen_addr();
    c2.advertised_listener = data_listen_addr().to_string();
    c2.controller_listen_addr = ctrl_addrs[1];
    c2.controller_quorum_voters = controller_voters(&voters);
    c2.bootstrap_mode = BootstrapMode::Bootstrap;
    c2.controller_listener_protocol = ListenerProtocol::Plaintext;

    // Static bootstrap: both brokers boot with the same voter set and elect
    // over the plaintext controller wire — no add_learner / change_membership.
    let c2_for_spawn = c2.clone();
    let join = tokio::spawn(async move {
        Broker::start_with_controller_listener(c2_for_spawn, Some(ctrl_l2)).await
    });
    let b1 = Broker::start_with_controller_listener(c1, Some(ctrl_l1))
        .await
        .expect("start b1");

    let b2 = join.await.expect("join spawn").expect("start b2");

    TwoBrokers {
        broker0: b1,
        broker1: b2,
        dir0: dir1,
        dir1: dir2,
    }
}

use {
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};

enum PeerSecurity {
    Plain,
    OAuth,
    Mismatched,
    Unauthorized,
    Anonymous,
}

#[derive(Default)]
struct ControllerAuthentication {
    security: Option<PeerSecurity>,
    brokers: Option<TwoBrokers>,
    token_directory: Option<TempDir>,
    registrations: Option<[usize; 2]>,
}

#[fixture]
fn controller_authentication() -> ControllerAuthentication {
    // Authentication policy is configured before observing actual peer registrations.
    ControllerAuthentication::default()
}

#[given("controller peers use matching SASL PLAIN credentials")]
fn matching_plain(controller_authentication: &mut ControllerAuthentication) {
    controller_authentication.security = Some(PeerSecurity::Plain);
}

#[given("controller peers use the same OAuth bearer identity")]
fn matching_oauth(controller_authentication: &mut ControllerAuthentication) {
    controller_authentication.security = Some(PeerSecurity::OAuth);
}

#[given("controller peers accept different SASL PLAIN users and passwords")]
fn mismatched_plain(controller_authentication: &mut ControllerAuthentication) {
    controller_authentication.security = Some(PeerSecurity::Mismatched);
}

#[given("controller peers authenticate with matching credentials but deny CLUSTER_ACTION")]
fn denied_controller_action(controller_authentication: &mut ControllerAuthentication) {
    controller_authentication.security = Some(PeerSecurity::Unauthorized);
}

#[given("controller peers use anonymous plaintext listeners")]
fn anonymous_controller_peers(controller_authentication: &mut ControllerAuthentication) {
    controller_authentication.security = Some(PeerSecurity::Anonymous);
}

#[when("the two controller listeners start with their configured peer security")]
async fn start_authenticated_controllers(controller_authentication: &mut ControllerAuthentication) {
    controller_authentication.brokers = Some(
        match controller_authentication
            .security
            .as_ref()
            .expect("peer security configured")
        {
            PeerSecurity::Plain => {
                Box::pin(start_two_brokers_with_controller_protocol(
                    ListenerProtocol::SaslPlaintext,
                    "broker",
                    "secret",
                ))
                .await
            }
            PeerSecurity::OAuth => {
                let (brokers, directory) = start_oauth_controllers().await;
                controller_authentication.token_directory = Some(directory);
                brokers
            }
            PeerSecurity::Mismatched => {
                Box::pin(start_independent_controllers(
                    [
                        PlainCredentials {
                            user: "alice",
                            password: "wonderland",
                        },
                        PlainCredentials {
                            user: "bob",
                            password: "burgers",
                        },
                    ],
                    ControllerAuthorization::Allowed,
                ))
                .await
            }
            PeerSecurity::Unauthorized => {
                Box::pin(start_independent_controllers(
                    [PlainCredentials {
                        user: "broker",
                        password: "secret",
                    }; 2],
                    ControllerAuthorization::Denied,
                ))
                .await
            }
            PeerSecurity::Anonymous => Box::pin(start_legacy_controllers()).await,
        },
    );
}

#[then("both controller peers observe two registered brokers")]
async fn authorized_registrations(controller_authentication: &ControllerAuthentication) {
    let brokers = controller_authentication
        .brokers
        .as_ref()
        .expect("controller listeners started");
    brokers.broker0.wait_until_brokers_registered(2).await;
    brokers.broker1.wait_until_brokers_registered(2).await;
}

#[when("peer registrations are observed after a three-second rejection window")]
async fn rejected_registration_window(controller_authentication: &mut ControllerAuthentication) {
    // Negative observation retains the original three-second interval.
    time::sleep(Duration::from_secs(3)).await;
    let brokers = controller_authentication
        .brokers
        .as_ref()
        .expect("controller listeners started");
    controller_authentication.registrations = Some([
        brokers.broker0.broker_count(),
        brokers.broker1.broker_count(),
    ]);
}

#[then("the first controller still observes fewer than two brokers")]
fn rejected_registrations(controller_authentication: &ControllerAuthentication) {
    let registrations = controller_authentication
        .registrations
        .expect("rejection window observed");
    assert!(
        registrations[0] < 2,
        "rejected peer unexpectedly registered: {registrations:?}"
    );
}

#[scenario(
    path = "tests/features/migrated/tests/raft_sasl.feature",
    name = "controller listener sasl plaintext two broker quorum"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_listener_sasl_plaintext_two_broker_quorum(
    controller_authentication: ControllerAuthentication,
) {
    controller_authentication.shutdown().await;
    drop(controller_authentication);
}

#[scenario(
    path = "tests/features/migrated/tests/raft_sasl.feature",
    name = "controller listener oauthbearer two broker quorum"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_listener_oauthbearer_two_broker_quorum(
    controller_authentication: ControllerAuthentication,
) {
    controller_authentication.shutdown().await;
    drop(controller_authentication);
}

#[scenario(
    path = "tests/features/migrated/tests/raft_sasl.feature",
    name = "controller listener sasl plaintext rejects mismatched creds"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_listener_sasl_plaintext_rejects_mismatched_creds(
    controller_authentication: ControllerAuthentication,
) {
    controller_authentication.shutdown().await;
    drop(controller_authentication);
}

#[scenario(
    path = "tests/features/migrated/tests/raft_sasl.feature",
    name = "controller listener sasl denies unauthorized principal"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_listener_sasl_denies_unauthorized_principal(
    controller_authentication: ControllerAuthentication,
) {
    controller_authentication.shutdown().await;
    drop(controller_authentication);
}

#[scenario(
    path = "tests/features/migrated/tests/raft_sasl.feature",
    name = "controller listener plaintext legacy path unchanged"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_listener_plaintext_legacy_path_unchanged(
    controller_authentication: ControllerAuthentication,
) {
    controller_authentication.shutdown().await;
    drop(controller_authentication);
}

impl ControllerAuthentication {
    async fn shutdown(&mut self) {
        let brokers = self.brokers.take().expect("controller peers running");
        if matches!(
            self.security,
            Some(PeerSecurity::Mismatched | PeerSecurity::Unauthorized)
        ) {
            brokers.broker1.shutdown().await;
            brokers.broker0.shutdown().await;
        } else {
            brokers.broker0.shutdown().await;
            brokers.broker1.shutdown().await;
        }
        drop((brokers.dir0, brokers.dir1));
        self.token_directory.take();
    }
}
