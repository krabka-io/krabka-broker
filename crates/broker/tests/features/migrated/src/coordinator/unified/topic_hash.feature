Feature: Kafka coordinator metadata hash compatibility
  Metadata changes drive group epochs and assignments. Topic and group hashes
  must therefore match Kafka's UUID layout, UTF-16 encoding, rack sorting,
  duplicate handling, and topic-name ordering exactly.

  Scenario Outline: Topic metadata hashes preserve Kafka's rack ordering and encoding
    Given topic <name> with UUID <id> has replica racks <racks>
    When the coordinator computes the topic metadata hash
    Then the topic hash equals Kafka's reference value <hash>

    Examples:
      | name  | id                                   | racks                                | hash                |
      | foo   | 01234567-89ab-cdef-fedc-ba9876543210   | [["rack1","rack0"],["rack2","rack1"]] | 4231092367357902416 |
      | bar   | 11111111-1111-1111-2222-222222222222   | [[]]                                 | 8733119781167439863 |
      | empty | 00000000-0000-0000-0000-000000000000   | []                                   | 728769691835767132  |
      | foo   | 01234567-89ab-cdef-fedc-ba9876543210   | [["r","r"]]                          | 2883780561648472600 |
      | foo   | 01234567-89ab-cdef-fedc-ba9876543210   | [["\\ue000","😀"]]                    | 4767223826197171634 |
      | té    | 01234567-89ab-cdef-fedc-ba9876543210   | [["é"]]                              | 480043309326700168  |

  Scenario Outline: Group metadata hashes are independent of topic iteration order
    Given the group topic hashes are <topics>
    When the coordinator computes the group metadata hash
    Then the group hash equals Kafka's reference value <hash>

    Examples:
      | topics                                                    | hash                 |
      | []                                                        | 0                    |
      | [["foo",4231092367357902416]]                              | -3651539661068893492 |
      | [["foo",4231092367357902416],["bar",8733119781167439863]]    | -556879919459959918  |
      | [["bar",8733119781167439863],["foo",4231092367357902416]]    | -556879919459959918  |
      | [["foo",456],["bar",123]]                                  | 1482712172080382793  |
