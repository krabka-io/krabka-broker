Feature: Kafka API dispatch coverage and quota accounting
  The advertised API catalog must match dispatchable schema APIs.
  Produce, Fetch, ShareFetch and ApiVersions account for their own request
  quotas. SaslHandshake, SaslAuthenticate, WriteTxnMarkers, AlterPartition,
  FetchSnapshot and UpdateRaftVoter are exempt, as are schema-less private APIs.
  Every other registered API uses dispatch accounting, including the private
  GetReplicaLogInfo API, which has a Kafka schema.

  Background:
    Given the broker's complete handler dispatch registry is assembled

  Scenario: Clients can dispatch every advertised API
    When the registered and advertised Kafka API keys are enumerated
    Then every advertised API is registered and every registered schema API is advertised

  Scenario: Handler registration preserves request-quota ownership
    When the dispatch request-quota policy for every registered API is read
    Then every registered API retains Kafka's request-quota accounting policy
