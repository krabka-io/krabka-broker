//! Tests for quota enforcement on the request path.
//!
//! Each test sets one quota for alice, drives the request the quota governs,
//! and asserts that the broker reports the delay to the client as a non-zero
//! `throttle_time_ms` (KIP-219 throttle-then-respond) instead of silently
//! stalling the connection.

use std::time::{Duration, Instant};

use assert2::assert;
use krabka_protocol::owned::{
    add_offsets_to_txn_response::AddOffsetsToTxnResponse,
    allocate_producer_ids_response::AllocateProducerIdsResponse,
};

use super::{
    cluster::{
        create_topic_as_admin, seed_alice_read_acl, seed_alice_write_acl,
        seed_compat_shim_disable_acl, start_single_broker_sasl_plaintext_with_users,
        wait_partition_exists,
    },
    data_plane::{
        drive_add_offsets_to_txn, drive_fetch_sasl, drive_produce_sasl,
        drive_unsupported_allocate_producer_ids,
    },
    quota_admin::drive_alter_client_quotas_sasl,
    wire::sasl_plain_authenticate,
};

/// The broker's default `quota_throttle_max`, in milliseconds. KIP-219 caps
/// the reported back-off at this, so no response may carry more.
const QUOTA_THROTTLE_MAX_MS: i32 = 1000;

/// Test 2: a low `(user=alice) producer_byte_rate` throttles a produce.
///
/// Set `(user=alice) producer_byte_rate=128`. alice produces about 8 KB. Assert
/// `throttle_time_ms > 0`.
///
/// Rate = 128 bytes/sec, burst = 1 second at rate = 128 bytes free. A produce
/// of 8 KB = 8192 bytes is about 7168 bytes over budget. At 128 bytes/sec that
/// is about 56 seconds of debt, but the response `throttle_time_ms` has a cap
/// of 1000ms. This test asserts only `throttle_time_ms` > 0. The exact value is
/// not load-bearing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn producer_byte_rate_throttles_produce() {
    let (handle, _dir, addr) = start_single_broker_sasl_plaintext_with_users(
        "admin",
        &[("admin", "admin-secret"), ("alice", "alice-secret")],
    )
    .await;

    // Seed ACL entries so the authorizer engages (compat shim disabled) and
    // alice can Write to the topic.
    seed_compat_shim_disable_acl(&handle).await;
    create_topic_as_admin(addr, "throttle-produce", 1, 1).await;
    wait_partition_exists(&handle, "throttle-produce", 0).await;
    seed_alice_write_acl(&handle, "throttle-produce").await;

    // Set low producer quota for alice.
    let alter_resp = drive_alter_client_quotas_sasl(
        addr,
        "admin",
        "admin-secret",
        vec![(
            vec![("user".into(), Some("alice".into()))],
            vec![("producer_byte_rate".into(), 128.0, false)],
        )],
        false,
    )
    .await;
    assert!(alter_resp[0].1 == 0, "alter quota must succeed");

    // Wait for the quota to appear in the image before producing.
    handle
        .wait_for_image(|img| {
            let key: krabka_metadata::EntityKey = vec![("user".into(), Some("alice".into()))];
            img.client_quotas()
                .get(&key)
                .and_then(|cfgs| cfgs.get("producer_byte_rate"))
                == Some(&128.0)
        })
        .await;

    // Alice produces 8 KB (8 records of 1 KB each). Rate = 128 bytes/sec.
    // Retry loop: TOPIC_AUTHORIZATION_FAILED (29) can fire if the alice ACL
    // hasn't propagated yet to the image snapshot used by the handler.
    let deadline = Instant::now() + Duration::from_secs(15);
    let resp = loop {
        let r =
            drive_produce_sasl(addr, "alice", b"alice-secret", "throttle-produce", 1024, 8).await;
        let ec = r
            .responses
            .first()
            .and_then(|t| t.partition_responses.first())
            .map_or(-1, |p| p.error_code);
        if ec != 29 {
            // Not TOPIC_AUTHORIZATION_FAILED — this is the response we want.
            break r;
        }
        assert!(
            Instant::now() <= deadline,
            "ACL still not applied after 15s; error_code=29"
        );
        // real-time wait (not a progress poll): retry cadence between network produce attempts (ACL propagation), deadline-guarded
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    let part = &resp.responses[0].partition_responses[0];
    assert!(
        part.error_code == 0,
        "produce must succeed, error_code={}",
        part.error_code
    );
    assert!(
        resp.throttle_time_ms > 0,
        "expected throttle_time_ms > 0, got {}",
        resp.throttle_time_ms
    );

    handle.shutdown().await;
}

/// Test 6: a tiny `(user=alice) request_percentage` throttles a produce.
///
/// Set a tiny `(user=alice) request_percentage` and NO byte-rate quota. alice
/// produces a small payload. Assert `throttle_time_ms > 0`.
///
/// This quota is the KIP-124 request quota, a server-side CPU-time throttle.
/// KIP-219 requires the broker to report it in `throttle_time_ms` and to mute
/// the channel. The broker must *not* silently delay the request.
/// `request_percentage=0.001` gives the bucket a budget of about 10µs/sec, far
/// below the processing time of any real produce handler. Even one small
/// produce thus trips the quota, and the response must carry a non-zero
/// throttle time. No `producer_byte_rate` is set, so only the request quota can
/// cause the throttle.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_percentage_throttles_produce() {
    let (handle, _dir, addr) = start_single_broker_sasl_plaintext_with_users(
        "admin",
        &[("admin", "admin-secret"), ("alice", "alice-secret")],
    )
    .await;

    seed_compat_shim_disable_acl(&handle).await;
    create_topic_as_admin(addr, "throttle-request", 1, 1).await;
    wait_partition_exists(&handle, "throttle-request", 0).await;
    seed_alice_write_acl(&handle, "throttle-request").await;

    // Set a tiny request_percentage for alice (no byte-rate quota).
    let alter_resp = drive_alter_client_quotas_sasl(
        addr,
        "admin",
        "admin-secret",
        vec![(
            vec![("user".into(), Some("alice".into()))],
            vec![("request_percentage".into(), 0.001, false)],
        )],
        false,
    )
    .await;
    assert!(alter_resp[0].1 == 0, "alter quota must succeed");

    // Wait for the quota to appear in the image before producing.
    handle
        .wait_for_image(|img| {
            let key: krabka_metadata::EntityKey = vec![("user".into(), Some("alice".into()))];
            img.client_quotas()
                .get(&key)
                .and_then(|cfgs| cfgs.get("request_percentage"))
                == Some(&0.001)
        })
        .await;

    // Alice produces a single small record. Retry past TOPIC_AUTHORIZATION_FAILED
    // (29) while the alice Write ACL propagates to the handler's image snapshot.
    let deadline = Instant::now() + Duration::from_secs(15);
    let resp = loop {
        let r = drive_produce_sasl(addr, "alice", b"alice-secret", "throttle-request", 16, 1).await;
        let ec = r
            .responses
            .first()
            .and_then(|t| t.partition_responses.first())
            .map_or(-1, |p| p.error_code);
        if ec != 29 {
            break r;
        }
        assert!(
            Instant::now() <= deadline,
            "ACL still not applied after 15s; error_code=29"
        );
        // real-time wait (not a progress poll): retry cadence between network produce attempts (ACL propagation), deadline-guarded
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    let part = &resp.responses[0].partition_responses[0];
    assert!(
        part.error_code == 0,
        "produce must succeed, error_code={}",
        part.error_code
    );
    assert!(
        resp.throttle_time_ms > 0,
        "expected request-quota throttle_time_ms > 0, got {}",
        resp.throttle_time_ms
    );

    handle.shutdown().await;
}

/// Renders the broker's registry as the exposition text an operator scrapes,
/// and reads one series' value out of it.
///
/// `Histogram::sum` and `Histogram::count` are behind prometheus-client's
/// `test-util` feature, which this workspace does not enable, so a test reads
/// a histogram the way Prometheus does. Missing series read as `0.0`: a
/// `Family` emits nothing until it has an entry, and "never observed" and
/// "observed only zeroes" mean the same thing to the assertions below.
async fn metric_value(handle: &krabka_broker::BrokerHandle, series: &str) -> f64 {
    let mut rendered = String::new();
    {
        let registry = handle.metrics().registry.lock().await;
        prometheus_client::encoding::text::encode(&mut rendered, &registry)
            .expect("encode registry");
    }
    rendered
        .lines()
        .find(|line| line.starts_with(series))
        .and_then(|line| line.rsplit(' ').next())
        .and_then(|value| value.parse().ok())
        .unwrap_or(0.0)
}

/// A throttled produce must move the throttle series, attributed to the quota
/// that caused it.
///
/// The setup is Test 2's: `(user=alice) producer_byte_rate=128`, and alice
/// produces 8 KB. That test asserts the client is *told* about the throttle;
/// this one asserts the broker records what it applied, so an operator seeing
/// produce latency rise can tell "the client is over its quota" from "the
/// broker is broken". `quota_type="Produce"` is the byte-rate quota, and it
/// must be the one credited: no `request_percentage` is set, so the KIP-124
/// quota asks for nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn producer_byte_rate_throttle_moves_the_throttle_metrics() {
    let (handle, _dir, addr) = start_single_broker_sasl_plaintext_with_users(
        "admin",
        &[("admin", "admin-secret"), ("alice", "alice-secret")],
    )
    .await;

    seed_compat_shim_disable_acl(&handle).await;
    create_topic_as_admin(addr, "throttle-metrics", 1, 1).await;
    wait_partition_exists(&handle, "throttle-metrics", 0).await;
    seed_alice_write_acl(&handle, "throttle-metrics").await;

    let alter_resp = drive_alter_client_quotas_sasl(
        addr,
        "admin",
        "admin-secret",
        vec![(
            vec![("user".into(), Some("alice".into()))],
            vec![("producer_byte_rate".into(), 128.0, false)],
        )],
        false,
    )
    .await;
    assert!(alter_resp[0].1 == 0, "alter quota must succeed");

    handle
        .wait_for_image(|img| {
            let key: krabka_metadata::EntityKey = vec![("user".into(), Some("alice".into()))];
            img.client_quotas()
                .get(&key)
                .and_then(|cfgs| cfgs.get("producer_byte_rate"))
                == Some(&128.0)
        })
        .await;

    // Alice produces 8 KB against a 128 B/s quota. Retry past
    // TOPIC_AUTHORIZATION_FAILED (29) while the Write ACL propagates.
    let deadline = Instant::now() + Duration::from_secs(15);
    let resp = loop {
        let r =
            drive_produce_sasl(addr, "alice", b"alice-secret", "throttle-metrics", 1024, 8).await;
        let ec = r
            .responses
            .first()
            .and_then(|t| t.partition_responses.first())
            .map_or(-1, |p| p.error_code);
        if ec != 29 {
            break r;
        }
        assert!(
            Instant::now() <= deadline,
            "ACL still not applied after 15s; error_code=29"
        );
        // real-time wait (not a progress poll): retry cadence between network produce attempts (ACL propagation), deadline-guarded
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert!(
        resp.throttle_time_ms > 0,
        "expected throttle_time_ms > 0, got {}",
        resp.throttle_time_ms
    );

    let applied_seconds = f64::from(resp.throttle_time_ms) / 1000.0;
    let phase = metric_value(
        &handle,
        "krabka_broker_request_throttle_duration_seconds_sum{api_key=\"Produce\"}",
    )
    .await;
    assert!(
        phase >= applied_seconds,
        "the throttle phase must cover the delay the response reported \
         ({applied_seconds}s); got {phase}"
    );

    // The byte-rate quota is what fired, so it is what the applied-throttle
    // family credits. The request quota is unset and must stay at zero.
    let by_quota = metric_value(
        &handle,
        "krabka_broker_quota_throttle_duration_seconds_sum{quota_type=\"Produce\"}",
    )
    .await;
    assert!(
        by_quota >= applied_seconds,
        "producer_byte_rate must be credited with the applied throttle \
         ({applied_seconds}s); got {by_quota}"
    );
    let request_quota = metric_value(
        &handle,
        "krabka_broker_quota_throttle_duration_seconds_sum{quota_type=\"Request\"}",
    )
    .await;
    assert!(
        request_quota < 1e-9,
        "no request_percentage quota is set, so it must be credited with \
         nothing; got {request_quota}"
    );

    handle.shutdown().await;
}

/// Test 3: a low `(user=alice) consumer_byte_rate` throttles a fetch.
///
/// Set `(user=alice) consumer_byte_rate=128`. Produce 8 KB as admin. alice
/// fetches. Assert `throttle_time_ms > 0`.
///
/// The rate and burst reasoning is the same as Test 2.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_byte_rate_throttles_fetch() {
    let (handle, _dir, addr) = start_single_broker_sasl_plaintext_with_users(
        "admin",
        &[("admin", "admin-secret"), ("alice", "alice-secret")],
    )
    .await;

    seed_compat_shim_disable_acl(&handle).await;
    create_topic_as_admin(addr, "throttle-fetch", 1, 1).await;
    wait_partition_exists(&handle, "throttle-fetch", 0).await;
    seed_alice_read_acl(&handle, "throttle-fetch").await;

    // Set low consumer quota for alice.
    let alter_resp = drive_alter_client_quotas_sasl(
        addr,
        "admin",
        "admin-secret",
        vec![(
            vec![("user".into(), Some("alice".into()))],
            vec![("consumer_byte_rate".into(), 128.0, false)],
        )],
        false,
    )
    .await;
    assert!(
        alter_resp[0].1 == 0,
        "alter consumer_byte_rate must succeed"
    );

    // Wait for the quota to appear in the image.
    handle
        .wait_for_image(|img| {
            let key: krabka_metadata::EntityKey = vec![("user".into(), Some("alice".into()))];
            img.client_quotas()
                .get(&key)
                .and_then(|cfgs| cfgs.get("consumer_byte_rate"))
                == Some(&128.0)
        })
        .await;

    // Produce 8 KB as admin (not subject to quota yet).
    seed_alice_write_acl(&handle, "throttle-fetch").await; // give admin path a topic
    let produce_resp =
        drive_produce_sasl(addr, "admin", b"admin-secret", "throttle-fetch", 1024, 8).await;
    let part = &produce_resp.responses[0].partition_responses[0];
    assert!(
        part.error_code == 0,
        "admin produce must succeed, error_code={}",
        part.error_code
    );

    // Alice fetches. Rate = 128 bytes/sec, data = 8 KB → throttle fires.
    // Retry loop: auth can lag.
    let deadline = Instant::now() + Duration::from_secs(15);
    let fetch_resp = loop {
        let r = drive_fetch_sasl(addr, "alice", b"alice-secret", "throttle-fetch").await;
        // If throttle_time_ms > 0 or error_code == 0, we have a real response.
        if r.error_code == 0 {
            break r;
        }
        assert!(
            Instant::now() <= deadline,
            "fetch error after 15s; error_code={}",
            r.error_code
        );
        // real-time wait (not a progress poll): retry cadence between network fetch attempts, deadline-guarded
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    assert!(
        fetch_resp.throttle_time_ms > 0,
        "expected consumer throttle_time_ms > 0, got {}",
        fetch_resp.throttle_time_ms
    );

    handle.shutdown().await;
}

/// Test 4: a `(user, client-id)` quota overrides a user-only quota.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn user_client_tuple_overrides_user_specific() {
    let (handle, _dir, addr) = start_single_broker_sasl_plaintext_with_users(
        "admin",
        &[("admin", "admin-secret"), ("alice", "alice-secret")],
    )
    .await;

    seed_compat_shim_disable_acl(&handle).await;
    create_topic_as_admin(addr, "precedence-topic", 1, 1).await;
    wait_partition_exists(&handle, "precedence-topic", 0).await;
    seed_alice_write_acl(&handle, "precedence-topic").await;

    // Set a lenient user-only quota.
    let alter_user = drive_alter_client_quotas_sasl(
        addr,
        "admin",
        "admin-secret",
        vec![(
            vec![("user".into(), Some("alice".into()))],
            vec![("producer_byte_rate".into(), 8192.0, false)],
        )],
        false,
    )
    .await;
    assert!(alter_user[0].1 == 0, "alter user quota must succeed");

    // Set a tight tuple quota for the client id written by `round_trip`.
    let alter_tuple = drive_alter_client_quotas_sasl(
        addr,
        "admin",
        "admin-secret",
        vec![(
            vec![
                ("user".into(), Some("alice".into())),
                ("client-id".into(), Some("krabka-quota-test".into())),
            ],
            vec![("producer_byte_rate".into(), 128.0, false)],
        )],
        false,
    )
    .await;
    assert!(alter_tuple[0].1 == 0, "alter tuple quota must succeed");

    // Wait for both quotas to appear in the image.
    handle
        .wait_for_image(|img| {
            let user_key: krabka_metadata::EntityKey = vec![("user".into(), Some("alice".into()))];
            let tuple_key: krabka_metadata::EntityKey = vec![
                ("client-id".into(), Some("krabka-quota-test".into())),
                ("user".into(), Some("alice".into())),
            ];
            let user_rate = img
                .client_quotas()
                .get(&user_key)
                .and_then(|c| c.get("producer_byte_rate"))
                .copied();
            let tuple_rate = img
                .client_quotas()
                .get(&tuple_key)
                .and_then(|c| c.get("producer_byte_rate"))
                .copied();
            user_rate == Some(8192.0) && tuple_rate == Some(128.0)
        })
        .await;

    // Alice produces 8 KB with `krabka-quota-test`. The 128-byte tuple quota
    // throttles it; the 8192-byte user-only quota would fit in the burst window.
    let deadline = Instant::now() + Duration::from_secs(15);
    let resp = loop {
        let r =
            drive_produce_sasl(addr, "alice", b"alice-secret", "precedence-topic", 1024, 8).await;
        let ec = r
            .responses
            .first()
            .and_then(|t| t.partition_responses.first())
            .map_or(-1, |p| p.error_code);
        if ec != 29 {
            break r;
        }
        assert!(
            Instant::now() <= deadline,
            "ACL still not applied after 15s; error_code=29"
        );
        // real-time wait (not a progress poll): retry cadence between network produce attempts (ACL propagation), deadline-guarded
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    let part = &resp.responses[0].partition_responses[0];
    assert!(
        part.error_code == 0,
        "produce must succeed, error_code={}",
        part.error_code
    );
    assert!(
        resp.throttle_time_ms > 0,
        "expected throttle_time_ms > 0 because the user/client tuple rate applies; got {}",
        resp.throttle_time_ms
    );

    handle.shutdown().await;
}

/// Test 7: the request quota is echoed on an API the dispatch loop patches.
///
/// `Produce` and `Fetch` fill `ThrottleTimeMs` in themselves, before encoding.
/// Everywhere else `maybe_apply_request_quota` runs, the delay is written into
/// the already-encoded body by patching its leading int32, which is only safe
/// on the responses whose schema really does put `ThrottleTimeMs` first.
/// `network::dispatch::throttle_audit` pins which those are against the
/// generated encoders; this test proves the patch reaches the wire on one of
/// them and corrupts nothing else.
///
/// `AddOffsetsToTxn` is the API driven here because its dispatch entry carries
/// `RequestQuotaPolicy::ApplyFallbackAccounting`, so the quota path runs, and
/// its response leads with `ThrottleTimeMs`, so the patch applies. alice drives
/// it with no quota set, then again with a tiny `(user=alice)`
/// `request_percentage`. The throttled response must report
/// `throttle_time_ms > 0` and be equal to the unthrottled one in every other
/// field, which is what a leading-int32 patch and nothing more looks like.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_percentage_throttle_is_echoed_on_a_patched_api() {
    let (handle, _dir, addr) = start_single_broker_sasl_plaintext_with_users(
        "admin",
        &[("admin", "admin-secret"), ("alice", "alice-secret")],
    )
    .await;

    // One connection for every alice request: re-authenticating would charge
    // each handshake to the same quota bucket.
    let mut stream = sasl_plain_authenticate(addr, "alice", b"alice-secret")
        .await
        .expect("SASL authenticate for AddOffsetsToTxn");

    // Baseline: no quota is configured yet, so there is no delay to report.
    let baseline = drive_add_offsets_to_txn(&mut stream, 10).await;
    assert!(
        baseline.throttle_time_ms == 0,
        "unthrottled response must report throttle_time_ms=0, got {}",
        baseline.throttle_time_ms
    );

    let alter_resp = drive_alter_client_quotas_sasl(
        addr,
        "admin",
        "admin-secret",
        vec![(
            vec![("user".into(), Some("alice".into()))],
            vec![("request_percentage".into(), 0.001, false)],
        )],
        false,
    )
    .await;
    assert!(alter_resp[0].1 == 0, "alter quota must succeed");

    handle
        .wait_for_image(|img| {
            let key: krabka_metadata::EntityKey = vec![("user".into(), Some("alice".into()))];
            img.client_quotas()
                .get(&key)
                .and_then(|cfgs| cfgs.get("request_percentage"))
                == Some(&0.001)
        })
        .await;

    // Drive the same request until the request bucket runs dry. Each request
    // charges its own handler time, so the first one over budget is throttled.
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut corr_id = 11;
    let throttled = loop {
        let resp = drive_add_offsets_to_txn(&mut stream, corr_id).await;
        corr_id += 1;
        if resp.throttle_time_ms > 0 {
            break resp;
        }
        assert!(
            Instant::now() <= deadline,
            "no request-quota throttle on AddOffsetsToTxn after 15s"
        );
    };

    // The patch touched the leading int32 and nothing else: every other field
    // still decodes to what the unthrottled response carried.
    assert!(
        throttled
            == AddOffsetsToTxnResponse {
                throttle_time_ms: throttled.throttle_time_ms,
                ..baseline
            }
    );

    handle.shutdown().await;
}

/// Test 8: the request-quota patch uses the version the reply was encoded at,
/// not the version the client asked for.
///
/// `send_unsupported_version` answers at the *nearest supported* version, so
/// the reply's schema version and header flexibility are not the request's.
/// `AllocateProducerIds` exists at v0 only and is flexible from v0: a request
/// at version -1 parses with a non-flexible request header and is answered
/// with a flexible v0 body, whose response header carries an extra
/// tagged-fields byte. Its response also leads with `ThrottleTimeMs`, so the
/// dispatch loop reports a request-quota delay by patching that leading int32
/// in place.
///
/// Reading the offset from the request header instead of the reply's puts the
/// patch one byte early: it overwrites the header's tagged-fields byte and
/// three of the four throttle bytes, leaving the fourth behind. The response
/// still decodes, so the corruption shows up as a `throttle_time_ms` far above
/// the broker's own cap rather than as a decode failure -- which is why this
/// test asserts the reported back-off is within `quota_throttle_max` and that
/// the reply is otherwise the unsupported-version response verbatim.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_quota_patch_uses_the_reply_version_not_the_request_version() {
    let (handle, _dir, addr) = start_single_broker_sasl_plaintext_with_users(
        "admin",
        &[("admin", "admin-secret"), ("alice", "alice-secret")],
    )
    .await;

    // One connection for every alice request: re-authenticating would charge
    // each handshake to the same quota bucket.
    let mut stream = sasl_plain_authenticate(addr, "alice", b"alice-secret")
        .await
        .expect("SASL authenticate for AllocateProducerIds");

    // Baseline: no quota is configured yet, so there is no delay to report and
    // the reply is untouched by the patch.
    let baseline = drive_unsupported_allocate_producer_ids(&mut stream, 10).await;
    assert!(
        baseline
            == AllocateProducerIdsResponse {
                error_code: 35, // UNSUPPORTED_VERSION
                ..Default::default()
            }
    );

    let alter_resp = drive_alter_client_quotas_sasl(
        addr,
        "admin",
        "admin-secret",
        vec![(
            vec![("user".into(), Some("alice".into()))],
            vec![("request_percentage".into(), 0.001, false)],
        )],
        false,
    )
    .await;
    assert!(alter_resp[0].1 == 0, "alter quota must succeed");

    handle
        .wait_for_image(|img| {
            let key: krabka_metadata::EntityKey = vec![("user".into(), Some("alice".into()))];
            img.client_quotas()
                .get(&key)
                .and_then(|cfgs| cfgs.get("request_percentage"))
                == Some(&0.001)
        })
        .await;

    // Drive the same rejected request until the request bucket runs dry.
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut corr_id = 11;
    let throttled = loop {
        let resp = drive_unsupported_allocate_producer_ids(&mut stream, corr_id).await;
        corr_id += 1;
        if resp.throttle_time_ms > 0 {
            break resp;
        }
        assert!(
            Instant::now() <= deadline,
            "no request-quota throttle on an unsupported-version reply after 15s"
        );
    };

    assert!(
        throttled.throttle_time_ms <= QUOTA_THROTTLE_MAX_MS,
        "throttle_time_ms={} exceeds the {QUOTA_THROTTLE_MAX_MS}ms cap, so the \
         patch wrote at the wrong offset",
        throttled.throttle_time_ms
    );
    // The patch touched the leading int32 of the reply and nothing else.
    assert!(
        throttled
            == AllocateProducerIdsResponse {
                throttle_time_ms: throttled.throttle_time_ms,
                ..baseline
            }
    );

    handle.shutdown().await;
}
