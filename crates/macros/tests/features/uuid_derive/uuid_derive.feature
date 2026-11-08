Feature: Kafka UUID derive input contract
  Scenario Outline: Only a non-generic tuple newtype can derive KafkaUuid
    Given UUID wrapper source <source>
    When the KafkaUuid derive is expanded
    Then the wrapper shape is rejected

    Examples:
      | case                         | source                             |
      | a unit wrapper               | struct Id;                         |
      | an empty braced wrapper      | struct Id {}                       |
      | a named UUID field           | struct Id { value: Uuid }          |
      | two UUID fields              | struct Id(Uuid, Uuid);             |
      | a const generic parameter    | struct Id<const DOMAIN: u8>(Uuid); |

  Scenario: A UUID newtype receives the complete string codec interface
    Given a UUID wrapper declared as
      """
      pub struct Id(pub Uuid);
      """
    When the KafkaUuid derive is expanded
    Then the wrapper implements Display FromStr Serialize and Deserialize
