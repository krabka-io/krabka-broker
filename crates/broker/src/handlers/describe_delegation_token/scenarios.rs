//! Step definitions for `visibility.feature`: which tokens
//! `DescribeDelegationToken` shows a caller, driven through [`super::handle`]
//! against a live single-voter controller.

use {
    super::handle,
    crate::{
        authorizer::{AllowAllAuthorizer, Authorizer, SimpleAclAuthorizer},
        codes,
        handlers::delegation_token_test_support::{
            TokenFixture, anonymous, authed, kp, seed_token, token_authed,
        },
        network::auth::ConnectionAuth,
        test_support::allow_acl,
    },
    assert2::assert,
    krabka_bdd::{Given, Outcome, ScenarioSpec, World, table},
    krabka_metadata::{
        AclEntry, AclOperation, MetadataRecord, PatternType, PermissionType, ResourceType,
    },
    krabka_protocol::owned::{
        describe_delegation_token_request::{
            DescribeDelegationTokenOwner, DescribeDelegationTokenRequest,
        },
        describe_delegation_token_response::DescribeDelegationTokenResponse,
    },
    std::{
        collections::{BTreeSet, HashSet},
        convert::Infallible,
        net::SocketAddr,
        str::FromStr,
    },
    tokio::sync::OnceCell,
};

/// The error a refused description answers, by its Kafka name.
#[derive(Clone, Copy, Debug, krabka_bdd::StepParam)]
enum Refusal {
    #[step_param("DELEGATION_TOKEN_REQUEST_NOT_ALLOWED")]
    RequestNotAllowed,
    #[step_param("DELEGATION_TOKEN_AUTH_DISABLED")]
    AuthDisabled,
}

impl Refusal {
    const fn code(self) -> i16 {
        match self {
            Self::RequestNotAllowed => codes::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            Self::AuthDisabled => codes::DELEGATION_TOKEN_AUTH_DISABLED,
        }
    }
}

/// Whether an ACL allows or denies its operation.
#[derive(Clone, Copy, Debug, krabka_bdd::StepParam)]
enum Permission {
    Allow,
    Deny,
}

/// An operation on a `User` resource, by its Kafka name.
#[derive(Clone, Copy, Debug, krabka_bdd::StepParam)]
enum UserOperation {
    #[step_param("All")]
    All,
    #[step_param("CreateTokens")]
    CreateTokens,
    #[step_param("DescribeTokens")]
    DescribeTokens,
}

/// One ACL on a `User` resource, written `<principal> <allow|deny> <operation>`.
#[derive(Debug)]
struct UserAcl {
    principal: String,
    permission: Permission,
    operation: UserOperation,
}

impl FromStr for UserAcl {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let mut words = text.split_whitespace();
        let (Some(principal), Some(permission), Some(operation), None) =
            (words.next(), words.next(), words.next(), words.next())
        else {
            return Err(format!(
                "`{text}` is not `<principal> <allow|deny> <operation>`"
            ));
        };
        Ok(Self {
            principal: principal.to_owned(),
            permission: permission
                .parse::<Permission>()
                .map_err(|error| error.to_string())?,
            operation: operation
                .parse::<UserOperation>()
                .map_err(|error| error.to_string())?,
        })
    }
}

/// The ACLs a step puts on one user resource, separated by `;`.
#[derive(Debug)]
struct UserAcls(Vec<UserAcl>);

impl FromStr for UserAcls {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        text.split(';')
            .map(|acl| acl.trim().parse())
            .collect::<Result<_, _>>()
            .map(Self)
    }
}

/// Token ids separated by `,`.
#[derive(Debug)]
struct TokenIds(BTreeSet<String>);

impl FromStr for TokenIds {
    type Err = Infallible;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Ok(Self(text.split(',').map(str::to_owned).collect()))
    }
}

/// The `owners` filter of a request: `anyone` for none, else user names
/// separated by `,`.
#[derive(Debug)]
struct OwnerFilter(Option<Vec<String>>);

impl FromStr for OwnerFilter {
    type Err = Infallible;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Ok(Self(
            (text != "anyone").then(|| text.split(',').map(str::to_owned).collect()),
        ))
    }
}

/// The authorizer the broker consults for token ACLs.
#[derive(Clone, Copy, Default)]
enum AuthorizerSetup {
    /// ACLs decide, and a resource without ACLs is denied.
    #[default]
    AclsOnly,
    /// Every operation is allowed.
    AllowAll,
    /// A resource without ACLs is allowed, and every `Describe` on a
    /// `DelegationToken` resource is denied, so only a `User` grant counts.
    AllowWithoutAclsDenyingTokenIds,
}

/// A description answered to one caller.
struct Description {
    caller: String,
    response: DescribeDelegationTokenResponse,
}

/// The broker state a description runs against, and what it answered.
struct TokenVisibility {
    cluster: OnceCell<TokenFixture>,
    secret_configured: bool,
    authorizer: AuthorizerSetup,
    description: Given<Description>,
}

#[rstest::fixture]
fn token_visibility() -> TokenVisibility {
    TokenVisibility {
        cluster: OnceCell::new(),
        secret_configured: true,
        authorizer: AuthorizerSetup::default(),
        description: Given::new("description", "When alice describes tokens owned by anyone"),
    }
}

impl World for TokenVisibility {
    async fn after(&mut self, _scenario: &ScenarioSpec, _outcome: &Outcome) {
        if let Some(cluster) = self.cluster.get() {
            cluster.controller.cancel().await;
        }
    }
}

impl TokenVisibility {
    async fn cluster(&self) -> &TokenFixture { self.cluster.get_or_init(TokenFixture::new).await }

    async fn seed_acl(&self, entry: AclEntry) {
        self.cluster()
            .await
            .controller
            .submit_change(vec![MetadataRecord::V1AccessControlEntry(entry)])
            .await
            .expect("seed acl");
    }

    async fn describe(
        &mut self,
        caller: &str,
        auth: &ConnectionAuth,
        owners: Option<Vec<DescribeDelegationTokenOwner>>,
    ) {
        let peer: SocketAddr = "127.0.0.1:0".parse().expect("static peer");
        let acls_only = SimpleAclAuthorizer::new(HashSet::new());
        let allow_without_acls =
            SimpleAclAuthorizer::new(HashSet::new()).with_allow_everyone_if_no_acl_found(true);
        let authorizer: &dyn Authorizer = match self.authorizer {
            AuthorizerSetup::AclsOnly => &acls_only,
            AuthorizerSetup::AllowAll => &AllowAllAuthorizer,
            AuthorizerSetup::AllowWithoutAclsDenyingTokenIds => &allow_without_acls,
        };
        let cluster = self.cluster().await;
        let response = handle(
            &DescribeDelegationTokenRequest {
                owners,
                ..Default::default()
            },
            auth,
            self.secret_configured.then_some(&cluster.secret),
            &*cluster.controller,
            &peer,
            authorizer,
        );
        self.description.set(Description {
            caller: caller.to_owned(),
            response,
        });
    }
}

async fn seed_owned(
    visibility: &TokenVisibility,
    owner: &str,
    token: &str,
    requester: Option<&str>,
    renewer: Option<&str>,
) {
    seed_token()
        .controller(&visibility.cluster().await.controller)
        .token_id(token)
        .owner(kp(owner))
        .maybe_requester(requester.map(kp))
        .renewers(renewer.map(kp).into_iter().collect())
        .call()
        .await;
}

#[krabka_bdd::given("{owner} owns token(s) {tokens}")]
async fn owner_owns_tokens(
    token_visibility: &mut TokenVisibility,
    owner: String,
    tokens: TokenIds,
) {
    for token in &tokens.0 {
        seed_owned(token_visibility, &owner, token, None, None).await;
    }
}

#[krabka_bdd::given("{owner} owns token {token} renewable by {renewer}")]
async fn owner_owns_renewable_token(
    token_visibility: &mut TokenVisibility,
    owner: String,
    token: String,
    renewer: String,
) {
    seed_owned(token_visibility, &owner, &token, None, Some(&renewer)).await;
}

/// One row of a token table: a token and its owner.
#[derive(Debug, krabka_bdd::TableRow)]
struct OwnedToken {
    token: String,
    owner: String,
}

#[krabka_bdd::given("these tokens exist")]
async fn these_tokens_exist(token_visibility: &mut TokenVisibility, datatable: Vec<Vec<String>>) {
    let rows: Vec<OwnedToken> = table::rows(&datatable).expect("token table");
    for OwnedToken { token, owner } in rows {
        seed_owned(token_visibility, &owner, &token, None, None).await;
    }
}

#[krabka_bdd::given("{requester} minted token {token} for {owner} renewable by {renewer}")]
async fn requester_minted_token(
    token_visibility: &mut TokenVisibility,
    requester: String,
    token: String,
    owner: String,
    renewer: String,
) {
    seed_owned(
        token_visibility,
        &owner,
        &token,
        Some(&requester),
        Some(&renewer),
    )
    .await;
}

#[krabka_bdd::given("{principal} may describe delegation token {token}")]
async fn principal_may_describe_token(
    token_visibility: &mut TokenVisibility,
    principal: String,
    token: String,
) {
    token_visibility
        .seed_acl(
            allow_acl()
                .resource_type(ResourceType::DelegationToken)
                .resource_name(&token)
                .principal(&format!("User:{principal}"))
                .operation(AclOperation::Describe)
                .call(),
        )
        .await;
}

#[krabka_bdd::given("user {owner} carries the ACLs {acls}")]
async fn user_carries_acls(token_visibility: &mut TokenVisibility, owner: String, acls: UserAcls) {
    for acl in acls.0 {
        token_visibility
            .seed_acl(AclEntry {
                resource_type: ResourceType::User,
                resource_name: format!("User:{owner}"),
                pattern_type: PatternType::Literal,
                principal: format!("User:{}", acl.principal),
                host: "*".into(),
                operation: match acl.operation {
                    UserOperation::All => AclOperation::All,
                    UserOperation::CreateTokens => AclOperation::CreateTokens,
                    UserOperation::DescribeTokens => AclOperation::DescribeTokens,
                },
                permission_type: match acl.permission {
                    Permission::Allow => PermissionType::Allow,
                    Permission::Deny => PermissionType::Deny,
                },
            })
            .await;
    }
}

#[krabka_bdd::given("every operation is allowed by the authorizer")]
fn every_operation_allowed(token_visibility: &mut TokenVisibility) {
    token_visibility.authorizer = AuthorizerSetup::AllowAll;
}

#[krabka_bdd::given(
    "the authorizer allows a resource without ACLs but no principal may describe a token by its id"
)]
async fn allow_without_acls_denying_token_ids(token_visibility: &mut TokenVisibility) {
    token_visibility.authorizer = AuthorizerSetup::AllowWithoutAclsDenyingTokenIds;
    let mut deny = allow_acl()
        .resource_type(ResourceType::DelegationToken)
        .resource_name("*")
        .principal("User:*")
        .operation(AclOperation::Describe)
        .call();
    deny.permission_type = PermissionType::Deny;
    token_visibility.seed_acl(deny).await;
}

#[krabka_bdd::given("the broker has no delegation token secret key")]
fn no_secret_key(token_visibility: &mut TokenVisibility) {
    token_visibility.secret_configured = false;
}

#[krabka_bdd::when("{caller} describes tokens owned by {owners}")]
async fn caller_describes_tokens(
    token_visibility: &mut TokenVisibility,
    caller: String,
    owners: OwnerFilter,
) {
    let owners = owners.0.map(|names| {
        names
            .into_iter()
            .map(|name| DescribeDelegationTokenOwner {
                principal_type: "User".into(),
                principal_name: name,
                ..Default::default()
            })
            .collect()
    });
    token_visibility
        .describe(&caller, &authed(&caller), owners)
        .await;
}

#[krabka_bdd::when("{caller} describes tokens with an empty owner list")]
async fn caller_describes_with_empty_owner_list(
    token_visibility: &mut TokenVisibility,
    caller: String,
) {
    token_visibility
        .describe(&caller, &authed(&caller), Some(Vec::new()))
        .await;
}

#[krabka_bdd::when("{caller} describes tokens on a connection authenticated by a delegation token")]
async fn token_authenticated_caller_describes(
    token_visibility: &mut TokenVisibility,
    caller: String,
) {
    token_visibility
        .describe(&caller, &token_authed(&caller), None)
        .await;
}

#[krabka_bdd::when("an unauthenticated caller describes tokens")]
async fn unauthenticated_caller_describes(token_visibility: &mut TokenVisibility) {
    token_visibility
        .describe("ANONYMOUS", &anonymous(), None)
        .await;
}

#[krabka_bdd::then("the description is refused with {refusal}")]
fn description_refused(token_visibility: &TokenVisibility, refusal: Refusal) {
    assert!(
        token_visibility.description.get().response
            == DescribeDelegationTokenResponse {
                error_code: refusal.code(),
                ..Default::default()
            }
    );
}

/// Checks that the description answered `caller` with exactly `visible`.
fn check_seen(token_visibility: &TokenVisibility, caller: &str, visible: BTreeSet<String>) {
    let Description {
        caller: described_for,
        response,
    } = token_visibility.description.get();
    let seen: BTreeSet<String> = response
        .tokens
        .iter()
        .map(|token| token.token_id.clone())
        .collect();
    assert!((described_for.as_str(), response.error_code, seen) == (caller, codes::NONE, visible));
}

#[krabka_bdd::then("{caller} sees token(s) {visible}")]
fn caller_sees_tokens(token_visibility: &TokenVisibility, caller: String, visible: TokenIds) {
    check_seen(token_visibility, &caller, visible.0);
}

#[krabka_bdd::then("{caller} sees no token")]
fn caller_sees_no_token(token_visibility: &TokenVisibility, caller: String) {
    check_seen(token_visibility, &caller, BTreeSet::new());
}

krabka_bdd::scenarios!(
    "tests/features/migrated/src/handlers/describe_delegation_token",
    fixtures = [token_visibility: TokenVisibility],
    world = token_visibility,
    runtime = "tokio-multi-thread",
    workers = 2,
);
