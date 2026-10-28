Feature: Kafka quorum peer request encoding
  Peer requests use Kafka base64 cluster IDs and the singleton cluster-metadata
  partition. EndQuorumEpoch encodes directory-aware candidates in current versions
  and legacy preferred-successor IDs when downgraded to version zero.

  Scenario: Vote decoding accepts a Kafka base64 cluster identity
    Given a Kafka Vote request with base64 cluster ID AQIDBAUGBwgJCgsMDQ4PEA
    When the Kafka Vote body is decoded as a peer request
    Then the decoded Vote cluster ID is UUID 01020304-0506-0708-090a-0b0c0d0e0f10

  Scenario: BeginQuorumEpoch preserves its target voter and advertised leader endpoint
    Given a BeginQuorumEpoch for voter 2 with leader 5 at epoch 9 and endpoint c5 port 9093
    When the singleton quorum request is encoded at the current protocol version
    Then BeginQuorumEpoch uses Kafka's singleton metadata partition and complete voter and leader endpoint fields

  Scenario: EndQuorumEpoch preserves the preferred successor across protocol versions
    Given an EndQuorumEpoch from leader 1 at epoch 4 preferring voter 2
    When the singleton quorum request is encoded at the current protocol version
    And the encoded EndQuorumEpoch is downgraded to protocol version zero
    Then EndQuorumEpoch uses Kafka's current candidate identities and version-zero preferred successors
