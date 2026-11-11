Feature: Kafka UUID derive input contract
  Scenario Outline: Only a non-generic tuple newtype can derive KafkaUuid
    Given a UUID wrapper declared as
      """
      <source>
      """
    When the KafkaUuid derive is expanded
    Then the wrapper shape is rejected with <message>

    Examples:
      | case                      | source                            | message                                                |
      | a unit wrapper            | struct Id;                        | `KafkaUuid` needs a tuple struct with exactly one field |
      | an empty braced wrapper   | struct Id {}                      | `KafkaUuid` needs a tuple struct with exactly one field |
      | a named UUID field        | struct Id { value: Uuid }          | `KafkaUuid` needs a tuple struct with exactly one field |
      | two UUID fields           | struct Id(Uuid, Uuid);             | `KafkaUuid` needs a tuple struct with exactly one field |
      | a const generic parameter | struct Id<const DOMAIN: u8>(Uuid); | `KafkaUuid` needs a non-generic UUID wrapper            |
