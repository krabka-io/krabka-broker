Feature: Kafka-defined metadata record types in checkpoints
  Native and Krabka-specific metadata must serialize using record API keys that
  Kafka understands. Private state travels inside supported Kafka record layouts.

  Scenario Outline: Snapshot record families retain Kafka metadata compatibility
    Given a snapshot metadata image containing record family <family>
    When the metadata image is serialized into a KRaft snapshot
    Then the checkpoint contains data records and every data-record API key is Kafka-defined

    Examples:
      | family                 |
      | feature-level          |
      | diskless-offset-advance |
      | topic-write-freeze     |
      | break-glass-proposal   |
