//! `DescribeUserScramCredentials` (`api_key` 50, KIP-554 read half).

use {
    crate::{
        broker::Broker,
        codes::{CLUSTER_AUTHORIZATION_FAILED, DUPLICATE_RESOURCE, RESOURCE_NOT_FOUND},
        handlers::{RequestContext, cluster_describe_denied},
    },
    krabka_metadata::MetadataImage,
    krabka_protocol::owned::{
        describe_user_scram_credentials_request::{DescribeUserScramCredentialsRequest, UserName},
        describe_user_scram_credentials_response::{
            CredentialInfo, DescribeUserScramCredentialsResponse,
            DescribeUserScramCredentialsResult,
        },
    },
    krabka_security::SaslMechanism,
    std::collections::{HashMap, HashSet, hash_map::Entry},
};

/// Kafka's `ScramImage.DESCRIBE_DUPLICATE_USER`; the row message appends the
/// user name after `": "`.
const DESCRIBE_DUPLICATE_USER: &str =
    "Cannot describe SCRAM credentials for the same user twice in a single request";

/// Kafka's `ScramImage.DESCRIBE_USER_THAT_DOES_NOT_EXIST`; the row message
/// appends the user name after `": "`.
const DESCRIBE_USER_THAT_DOES_NOT_EXIST: &str =
    "Attempt to describe a user credential that does not exist";

pub fn handle(
    broker: &Broker,
    req: &DescribeUserScramCredentialsRequest,
    _version: i16,
    ctx: &RequestContext<'_>,
) -> DescribeUserScramCredentialsResponse {
    let image = broker.controller.current_image();

    if cluster_describe_denied(broker.config.authorizer.as_ref(), &image, ctx) {
        return denied_response(req);
    }

    let known_users: HashSet<String> = image.scram_credentials_users().into_iter().collect();
    let targets = requested_targets(&known_users, req.users.as_deref());

    let results = build_results(&image, &known_users, targets);

    DescribeUserScramCredentialsResponse {
        throttle_time_ms: 0,
        error_code: 0,
        error_message: None,
        results,
        ..Default::default()
    }
}

/// The refusal Kafka's `DescribeUserScramCredentialsRequest.getErrorResponse`
/// builds for a `CLUSTER_AUTHORIZATION_FAILED` exception.
///
/// `ApiError.fromThrowable` drops a message equal to the error's default text,
/// so the top level carries no message. Each requested user gets one row with
/// the same code and an empty user name, because Kafka never sets `user` on
/// those rows. A null user list gives no rows.
fn denied_response(
    req: &DescribeUserScramCredentialsRequest,
) -> DescribeUserScramCredentialsResponse {
    let results = req
        .users
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|_| DescribeUserScramCredentialsResult {
            user: String::new(),
            error_code: CLUSTER_AUTHORIZATION_FAILED,
            error_message: None,
            ..Default::default()
        })
        .collect();
    DescribeUserScramCredentialsResponse {
        throttle_time_ms: 0,
        error_code: CLUSTER_AUTHORIZATION_FAILED,
        error_message: None,
        results,
        ..Default::default()
    }
}

fn build_results(
    image: &MetadataImage,
    known_users: &HashSet<String>,
    targets: Vec<DescribeTarget>,
) -> Vec<DescribeUserScramCredentialsResult> {
    targets
        .into_iter()
        .map(|target| {
            let user = target.user;
            if target.is_duplicate {
                return DescribeUserScramCredentialsResult {
                    error_message: Some(format!("{DESCRIBE_DUPLICATE_USER}: {user}")),
                    user,
                    error_code: DUPLICATE_RESOURCE,
                    credential_infos: vec![],
                    ..Default::default()
                };
            }

            let mut pairs = image.scram_credentials_for_user(&user);
            if pairs.is_empty() && !known_users.contains(&user) {
                DescribeUserScramCredentialsResult {
                    error_code: RESOURCE_NOT_FOUND,
                    error_message: Some(format!("{DESCRIBE_USER_THAT_DOES_NOT_EXIST}: {user}")),
                    user,
                    credential_infos: vec![],
                    ..Default::default()
                }
            } else {
                pairs.sort_by_key(|(mech, _)| sasl_mechanism_to_byte(*mech));
                let credential_infos: Vec<CredentialInfo> = pairs
                    .into_iter()
                    .map(|(mech, iters)| CredentialInfo {
                        mechanism: sasl_mechanism_to_byte(mech),
                        iterations: iters.cast_signed(),
                        ..Default::default()
                    })
                    .collect();
                DescribeUserScramCredentialsResult {
                    user,
                    error_code: 0,
                    error_message: None,
                    credential_infos,
                    ..Default::default()
                }
            }
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DescribeTarget {
    user: String,
    is_duplicate: bool,
}

fn requested_targets(
    known_users: &HashSet<String>,
    users_filter: Option<&[UserName]>,
) -> Vec<DescribeTarget> {
    let Some(filter) = users_filter else {
        return all_known_user_targets(known_users);
    };
    if filter.is_empty() {
        return all_known_user_targets(known_users);
    }

    let mut requested_users = Vec::new();
    let mut duplicate_flags = HashMap::new();
    for requested_user in filter {
        let user = requested_user.name.clone();
        match duplicate_flags.entry(user.clone()) {
            Entry::Vacant(entry) => {
                entry.insert(false);
                requested_users.push(user);
            }
            Entry::Occupied(mut entry) => {
                entry.insert(true);
            }
        }
    }

    requested_users
        .into_iter()
        .map(|user| DescribeTarget {
            is_duplicate: duplicate_flags.get(&user).copied().unwrap_or(false),
            user,
        })
        .collect()
}

fn all_known_user_targets(known_users: &HashSet<String>) -> Vec<DescribeTarget> {
    let mut users: Vec<String> = known_users.iter().cloned().collect();
    users.sort();
    users
        .into_iter()
        .map(|user| DescribeTarget {
            user,
            is_duplicate: false,
        })
        .collect()
}

#[must_use]
fn sasl_mechanism_to_byte(m: SaslMechanism) -> i8 {
    match m {
        SaslMechanism::ScramSha256 => 1,
        SaslMechanism::ScramSha512 => 2,
        // Non-SCRAM mechanisms never own SCRAM credential records; map to the
        // KIP-554 UNKNOWN sentinel (0).
        SaslMechanism::Plain | SaslMechanism::OAuthBearer | SaslMechanism::Gssapi => 0,
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::assert,
        krabka_bdd::Given,
        krabka_metadata::{MetadataRecord, ScramCredentialRecord},
        std::{convert::Infallible, str::FromStr},
    };

    /// A SCRAM credential to seed: its user, mechanism and iteration count.
    #[derive(Clone)]
    struct SeededUser {
        user: String,
        mechanism: SaslMechanism,
        iterations: u32,
    }

    fn img_with_scram(users: &[SeededUser]) -> MetadataImage {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        for SeededUser {
            user,
            mechanism,
            iterations,
        } in users
        {
            img.apply(&MetadataRecord::V1ScramCredential(ScramCredentialRecord {
                user: user.clone(),
                mechanism: *mechanism,
                iterations: *iterations,
                salt: vec![1, 2, 3],
                server_key: vec![4, 5, 6],
                stored_key: vec![7, 8, 9],
            }));
        }
        img
    }

    krabka_broker_macros::scram_users_fixture!(scram_users_request);

    /// The per-user results the handler builds for `users_filter` over the
    /// credentials in `seeded`, past the authorization check.
    fn described_results(
        users_filter: Option<Vec<String>>,
        seeded: &[SeededUser],
    ) -> Vec<DescribeUserScramCredentialsResult> {
        let req = scram_users_request(users_filter);
        let image = img_with_scram(seeded);
        let known_users: HashSet<String> = image.scram_credentials_users().into_iter().collect();
        let targets = requested_targets(&known_users, req.users.as_deref());
        build_results(&image, &known_users, targets)
    }

    /// A step's user list: `null`, `empty`, or names separated by `,`.
    #[derive(Debug)]
    struct UserList(Option<Vec<String>>);

    impl FromStr for UserList {
        type Err = Infallible;

        fn from_str(text: &str) -> Result<Self, Self::Err> {
            Ok(Self(match text {
                "null" => None,
                "empty" => Some(Vec::new()),
                names => Some(names.split(',').map(str::to_owned).collect()),
            }))
        }
    }

    /// A SCRAM mechanism by its Kafka name.
    #[derive(Clone, Copy, Debug, krabka_bdd::StepParam)]
    enum Mechanism {
        #[step_param("SCRAM-SHA-256")]
        Sha256,
        #[step_param("SCRAM-SHA-512")]
        Sha512,
    }

    impl From<Mechanism> for SaslMechanism {
        fn from(mechanism: Mechanism) -> Self {
            match mechanism {
                Mechanism::Sha256 => Self::ScramSha256,
                Mechanism::Sha512 => Self::ScramSha512,
            }
        }
    }

    /// Successful rows written `<user> with <mechanism> at <n> iterations`,
    /// joined by ` and `.
    #[derive(Debug)]
    struct Listing(Vec<DescribeUserScramCredentialsResult>);

    impl FromStr for Listing {
        type Err = String;

        fn from_str(text: &str) -> Result<Self, Self::Err> {
            text.split(" and ")
                .map(|row| {
                    let shape =
                        || format!("`{row}` is not `<user> with <mechanism> at <n> iterations`");
                    let (user, credential) = row.split_once(" with ").ok_or_else(shape)?;
                    let (mechanism, iterations) = credential
                        .strip_suffix(" iterations")
                        .and_then(|rest| rest.split_once(" at "))
                        .ok_or_else(shape)?;
                    let mechanism: Mechanism =
                        mechanism.parse().map_err(|error| format!("{error:?}"))?;
                    Ok(krabka_broker_macros::tagged_wire!(
                        DescribeUserScramCredentialsResult {
                            user: user.to_owned(),
                            error_code: 0,
                            error_message: None,
                            credential_infos: vec![krabka_broker_macros::tagged_wire!(
                                CredentialInfo {
                                    mechanism: sasl_mechanism_to_byte(mechanism.into()),
                                    iterations: iterations.parse().map_err(|_| "iterations")?,
                                }
                            )],
                        }
                    ))
                })
                .collect::<Result<_, String>>()
                .map(Self)
        }
    }

    /// An error row for `user` with Kafka's `code` and message.
    fn error_row(user: &str, code: i16, message: &str) -> DescribeUserScramCredentialsResult {
        krabka_broker_macros::tagged_wire!(DescribeUserScramCredentialsResult {
            user: user.to_owned(),
            error_code: code,
            error_message: Some(format!("{message}: {user}")),
            credential_infos: Vec::new(),
        })
    }

    struct DescribeScramScenario {
        seeded: Vec<SeededUser>,
        results: Given<Vec<DescribeUserScramCredentialsResult>>,
        denied: Given<DescribeUserScramCredentialsResponse>,
    }

    #[rstest::fixture]
    fn describe_scram() -> DescribeScramScenario {
        DescribeScramScenario {
            seeded: Vec::new(),
            results: Given::new(
                "results",
                "When a client describes the SCRAM credentials with user list <users>",
            ),
            denied: Given::new(
                "denied",
                "When a client without cluster Describe permission describes the SCRAM \
                 credentials with user list <users>",
            ),
        }
    }

    #[krabka_bdd::given("{user} has a {mechanism} credential with {iterations} iterations")]
    fn user_has_credential(
        describe_scram: &mut DescribeScramScenario,
        user: String,
        mechanism: Mechanism,
        iterations: u32,
    ) {
        describe_scram.seeded.push(SeededUser {
            user,
            mechanism: mechanism.into(),
            iterations,
        });
    }

    #[krabka_bdd::when("a client describes the SCRAM credentials with user list {users}")]
    fn client_describes_users(describe_scram: &mut DescribeScramScenario, users: UserList) {
        let results = described_results(users.0, &describe_scram.seeded);
        describe_scram.results.set(results);
    }

    #[krabka_bdd::when(
        "a client without cluster Describe permission describes the SCRAM credentials with user list {users}"
    )]
    fn denied_client_describes_users(describe_scram: &mut DescribeScramScenario, users: UserList) {
        let response = denied_response(&scram_users_request(users.0));
        describe_scram.denied.set(response);
    }

    #[krabka_bdd::then("{user}'s credentials are listed as {first} then {second}")]
    fn credentials_listed_in_order(
        describe_scram: &DescribeScramScenario,
        user: String,
        first: Mechanism,
        second: Mechanism,
    ) {
        let mechanisms: Vec<Vec<i8>> = describe_scram
            .results
            .get()
            .iter()
            .filter(|row| row.user == user)
            .map(|row| {
                row.credential_infos
                    .iter()
                    .map(|info| info.mechanism)
                    .collect()
            })
            .collect();
        let expected = vec![vec![
            sasl_mechanism_to_byte(first.into()),
            sasl_mechanism_to_byte(second.into()),
        ]];
        assert!(mechanisms == expected);
    }

    #[krabka_bdd::then("the description lists {listing}")]
    fn description_lists(describe_scram: &DescribeScramScenario, listing: Listing) {
        assert!(describe_scram.results.get() == &listing.0);
    }

    #[krabka_bdd::then("the description reports {user} as not found")]
    fn description_reports_not_found(describe_scram: &DescribeScramScenario, user: String) {
        let expected = vec![error_row(
            &user,
            RESOURCE_NOT_FOUND,
            "Attempt to describe a user credential that does not exist",
        )];
        assert!(describe_scram.results.get() == &expected);
    }

    #[krabka_bdd::then("the description reports {user} as a duplicate and lists {listing}")]
    fn description_reports_duplicate(
        describe_scram: &DescribeScramScenario,
        user: String,
        listing: Listing,
    ) {
        let mut expected = vec![error_row(
            &user,
            DUPLICATE_RESOURCE,
            "Cannot describe SCRAM credentials for the same user twice in a single request",
        )];
        expected.extend(listing.0);
        assert!(describe_scram.results.get() == &expected);
    }

    #[krabka_bdd::then(
        "the description fails with CLUSTER_AUTHORIZATION_FAILED in {rows} anonymous error rows"
    )]
    fn check_denied_scram(describe_scram: &DescribeScramScenario, rows: usize) {
        let denied_row = krabka_broker_macros::tagged_wire!(DescribeUserScramCredentialsResult {
            user: String::new(),
            error_code: CLUSTER_AUTHORIZATION_FAILED,
            error_message: None,
            credential_infos: Vec::new()
        });
        assert!(
            describe_scram.denied.get()
                == &krabka_broker_macros::unthrottled_wire!(DescribeUserScramCredentialsResponse {
                    error_code: CLUSTER_AUTHORIZATION_FAILED,
                    error_message: None,
                    results: vec![denied_row; rows]
                })
        );
    }

    krabka_bdd::scenarios!(
        "tests/features/handler_describe_scram",
        fixtures = [describe_scram: DescribeScramScenario],
    );

    #[yare::parameterized(
        scram_sha256 = { SaslMechanism::ScramSha256, 1 },
        scram_sha512 = { SaslMechanism::ScramSha512, 2 },
        plain = { SaslMechanism::Plain, 0 },
    )]
    fn sasl_mechanism_byte_mapping(mechanism: SaslMechanism, want: i8) {
        assert2::assert!(sasl_mechanism_to_byte(mechanism) == want, "{mechanism:?}");
    }
}
