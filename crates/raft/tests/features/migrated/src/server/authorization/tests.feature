Feature: Kafka controller authorization and refusal layouts
  The controller listener checks its own cluster operations. Broker-handled APIs
  keep their handler authorization. Refusals use each API's complete response shape,
  including Fetch session/partition fields and private metadata response sentinels.

  Scenario Outline: Each controller API requires its Kafka cluster operation
    Given controller listener API <api>
    When the controller listener resolves the API's required cluster operation
    Then the controller API requires <operation>

    Examples:
      | api | operation |
      | Fetch | ClusterAction |
      | Vote | ClusterAction |
      | BeginQuorumEpoch | ClusterAction |
      | EndQuorumEpoch | ClusterAction |
      | FetchSnapshot | ClusterAction |
      | ControllerRegistration | ClusterAction |
      | UpdateRaftVoter | ClusterAction |
      | AssignReplicasToDirs | ClusterAction |
      | SubmitChange | ClusterAction |
      | MetadataFetch | ClusterAction |
      | DelegationTokenMutation | ClusterAction |
      | AddRaftVoter | Alter |
      | RemoveRaftVoter | Alter |
      | DescribeCluster | Alter |
      | DescribeQuorum | Describe |
      | ApiVersions | broker-handler-authorization |
      | CreateTopics | broker-handler-authorization |
      | Envelope | broker-handler-authorization |
      | BrokerRegistration | broker-handler-authorization |
      | BrokerHeartbeat | broker-handler-authorization |

  Scenario: Fetch and quorum refusals retain exact Kafka schema layouts
    Given the controller receives these authorization-refusal request shapes:
      | api | version |
      | Fetch | 12 |
      | Fetch | 13 |
      | Fetch | 17 |
      | Vote | 2 |
      | BeginQuorumEpoch | 1 |
      | EndQuorumEpoch | 1 |
      | FetchSnapshot | 1 |
    When the controller encodes ClusterAuthorizationFailed for every requested API
    Then every refusal matches the complete Kafka response layout and consumes every encoded byte

  Scenario: Admin and private metadata refusals retain exact API error layouts
    Given the controller receives these authorization-refusal request shapes:
      | api | version |
      | AssignReplicasToDirs | 0 |
      | ControllerRegistration | 0 |
      | UpdateRaftVoter | 0 |
      | AddRaftVoter | 0 |
      | RemoveRaftVoter | 0 |
      | DescribeCluster | 1 |
      | DescribeQuorum | 2 |
      | SubmitChange | 0 |
      | DelegationTokenMutation | 0 |
      | MetadataFetch | 0 |
    When the controller encodes ClusterAuthorizationFailed for every requested API
    Then every refusal matches the complete Kafka response layout and consumes every encoded byte
