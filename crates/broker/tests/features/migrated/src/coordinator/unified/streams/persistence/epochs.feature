Feature: Streams group and assignment epoch tags
  Scenario Outline: Kafka production and trunk topology tags preserve null default and explicit values
    Given Streams group epoch 7 has validated topology <validated> configs <configs> stored description <stored> and failed description <failed>
    And Kafka represents that Streams epoch record with bytes <hex>
    When the coordinator writes the Streams epoch record and reads Kafka bytes
    Then the Streams epoch bytes match Kafka and all optional tags preserve their original values
    Examples:
      | validated | configs | stored | failed | hex |
      | -1 | null | -1 | -1 | 000000000007010203040506070800 |
      | 0 | num.standby.replicas=0 | -1 | -1 | 000000000007010203040506070802000400000000011902156e756d2e7374616e6462792e7265706c69636173023000 |
      | -1 | empty | -1 | -1 | 000000000007010203040506070801010101 |
      | 3 | a=1,b=22 | -1 | -1 | 000000000007010203040506070802000400000003010c030261023100026203323200 |
      | 4 | null | -1 | -1 | 000000000007010203040506070801000400000004 |
      | 4 | null | 4 | -1 | 000000000007010203040506070802000400000004020400000004 |
      | 4 | null | -2 | 3 | 0000000000070102030405060708030004000000040204fffffffe030400000003 |
      | 4 | null | -1 | 0 | 000000000007010203040506070802000400000004030400000000 |
  Scenario Outline: Assignment completion time is absent when unknown
    Given Streams assignment epoch 12 completed at timestamp <timestamp>
    And Kafka represents that Streams epoch record with bytes <hex>
    When the coordinator writes the Streams epoch record and reads Kafka bytes
    Then the Streams epoch bytes match Kafka and all optional tags preserve their original values
    Examples:
      | timestamp | hex |
      | 0 | 00000000000c00 |
      | 1791331200000 | 00000000000c010008000001a113a8ec00 |
