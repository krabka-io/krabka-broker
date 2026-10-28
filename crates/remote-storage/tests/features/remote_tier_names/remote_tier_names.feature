Feature: Kafka-compatible remote tier paths
  Scenario Outline: Remote index files use Kafka suffixes
    Given remote index kind <kind> with Kafka suffix <suffix>
    When the remote index suffix is rendered and parsed
    Then the remote index suffix and kind match Kafka in both directions

    Examples:
      | kind              | suffix                   |
      | offset            | .index                   |
      | timestamp         | .timeindex               |
      | producer-snapshot | .snapshot                |
      | leader-epoch      | .leader_epoch_checkpoint |
      | transaction       | .txnindex                |

  Scenario: Local tiered storage uses Kafka partition and snapshot names
    Given orders partition 7 starts at offset 11 with topic ID 1 and segment ID 254
    When remote tier names are rendered for suffix .snapshot
    Then the remote partition directory is orders-7-AAAAAAAAAAAAAAAAAAAAAQ
    And the remote segment file is 00000000000000000011-AAAAAAAAAAAAAAAAAAAA_g.snapshot

  Scenario: Object stores use Kafka partition and log names
    Given orders partition 0 starts at offset 0 with topic ID 1 and segment ID 30
    When remote tier names are rendered for suffix .log
    Then the remote partition directory is orders-0-AAAAAAAAAAAAAAAAAAAAAQ
    And the remote segment file is 00000000000000000000-AAAAAAAAAAAAAAAAAAAAHg.log

  Scenario Outline: Kafka remote names retain their IDs offsets and suffixes
    Given Kafka remote partition directory orders-7-AAAAAAAAAAAAAAAAAAAAAQ and segment file <file>
    When the Kafka remote tier names are parsed
    Then the names identify orders partition 7 with topic ID 1
    And the segment starts at <offset> with ID <segment> and suffix <suffix>

    Examples:
      | file                                                   | offset | segment | suffix    |
      | 00000000000000000011-AAAAAAAAAAAAAAAAAAAA_g.snapshot     | 11     | 254     | .snapshot |
      | 00000000000000000000-AAAAAAAAAAAAAAAAAAAAHg.log          | 0      | 30      | .log      |

  Scenario Outline: A remote Kafka UUID needs the exact base64 length
    Given Kafka UUID text <encoded>
    When the remote Kafka UUID is decoded
    Then the malformed Kafka UUID is rejected

    Examples:
      | encoded                  |
      | empty                    |
      | AAAA                     |
      | AAAAAAAAAAAAAAAAAAAAAQAA |
      | A                        |
