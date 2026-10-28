Feature: Kafka UUID derive input contract
  Scenario Outline: Only a non-generic tuple newtype can derive KafkaUuid
    Given UUID wrapper source <source>
    When the KafkaUuid derive is expanded
    Then the wrapper shape is rejected

    Examples:
      | source                                  |
      | struct Id;                              |
      | struct Id {}                            |
      | struct Id { value: Uuid }                |
      | struct Id(Uuid, Uuid);                   |
      | struct Id<const DOMAIN: u8>(Uuid);       |

  Scenario: A UUID newtype receives the complete string codec interface
    Given UUID wrapper source pub struct Id(pub Uuid);
    When the KafkaUuid derive is expanded
    Then the wrapper implements Display FromStr Serialize and Deserialize
