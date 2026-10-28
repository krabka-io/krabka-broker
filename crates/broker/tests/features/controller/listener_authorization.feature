Feature: Controller listeners authorize each request for its authenticated connection identity

  Scenario: Plaintext authorization is checked for each request on an open connection
    Given an open plaintext controller connection has principal ANONYMOUS and no ClusterAction grant
    When ANONYMOUS sends Vote, SubmitChange for anonymous-denied and BrokerHeartbeat for broker 1 at epoch -1 on that connection
    Then Vote and SubmitChange return their complete authorization failure shapes, the heartbeat returns code 31 and anonymous-denied is absent
    When User:ANONYMOUS receives ClusterAction and submits anonymous-allowed on the same controller connection
    Then SubmitChange succeeds and anonymous-allowed is committed without reconnecting

  Scenario: SASL principals receive only the controller operations they hold
    Given SASL PLAIN controller users creator, replicator and reader hold only cluster Create, ClusterAction and Describe respectively
    When creator creates topic created-by-creator on the controller connection
    Then the creation response contains exactly created-by-creator with success code 0
    When replicator fetches metadata from offset 0 with max bytes 1024 and then describes the metadata quorum
    Then MetadataFetch succeeds but replicator's complete DescribeQuorum response is an authorization failure
    When reader describes the metadata quorum and describes cluster endpoint type 2 on the same controller connection
    Then reader's DescribeQuorum succeeds but its complete DescribeCluster response is an authorization failure

  Scenario: TLS certificate identity authorizes each request on the same connection
    Given a mutually authenticated TLS controller connection uses the fixture client certificate principal without a Describe grant
    When the certificate principal sends DescribeQuorum without a Describe grant
    Then the complete certificate-authenticated DescribeQuorum response has authorization error 31 and Cluster authorization failed message
    When the certificate principal receives Describe and sends DescribeQuorum again on the same TLS connection
    Then the next certificate-authenticated DescribeQuorum response succeeds without reconnecting
