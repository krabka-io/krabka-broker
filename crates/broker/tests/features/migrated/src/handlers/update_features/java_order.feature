Feature: Kafka feature-validation error precedence
  UpdateFeatures checks stop at the first failing feature in Java HashMap
  iteration order, which can differ from the order submitted by the client.

  Scenario: Eight-feature requests retain Kafka error precedence
    Given an UpdateFeatures request lists these features in order:
      | name |
      | group.version |
      | share.version |
      | transaction.version |
      | streams.version |
      | eligible.leader.replicas.version |
      | kraft.version |
      | metadata.version |
      | no.such.feature |
    When the controller orders feature validation as Kafka's HashMap does
    Then the features are validated exactly in this order:
      | name |
      | group.version |
      | no.such.feature |
      | kraft.version |
      | share.version |
      | metadata.version |
      | streams.version |
      | transaction.version |
      | eligible.leader.replicas.version |

  Scenario: A later group version can fail before an earlier share version
    Given an UpdateFeatures request lists these features in order:
      | name |
      | share.version |
      | group.version |
    When the controller orders feature validation as Kafka's HashMap does
    Then the features are validated exactly in this order:
      | name |
      | group.version |
      | share.version |

  Scenario: Duplicate features keep their first position within a shared bucket
    Given an UpdateFeatures request lists these features in order:
      | name |
      | metadata.version |
      | share.version |
      | metadata.version |
    When the controller orders feature validation as Kafka's HashMap does
    Then the features are validated exactly in this order:
      | name |
      | metadata.version |
      | share.version |
