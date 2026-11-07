Feature: Legacy messages carry a CRC-protected magic 0 and magic 1 layout

  A legacy message is one entry of a Kafka MessageSet with magic 0 or
  magic 1, as Apache Kafka's LegacyRecord defines it. Each entry is an
  8-byte offset and a 4-byte message size, followed by the message: CRC,
  magic, attributes, a timestamp in magic 1 only (KIP-32), key and value.
  The hex in these scenarios is grouped field by field. Its CRCs were
  computed with an independent CRC-32 (IEEE) implementation.

  Rule: A magic 0 message carries no timestamp

    Scenario: A magic 0 message is laid out without a timestamp
      Given a record at offset 0 with a null key and the value hello
      When a legacy producer encodes it with magic 0
      Then the message set bytes are
        """
        0000000000000000 00000013 87a77ab2 00 00 ffffffff 00000005 68656c6c6f
        """

    Scenario: A magic 0 message decodes without a timestamp
      Given a message set from a legacy producer
        """
        0000000000000000 00000013 87a77ab2 00 00 ffffffff 00000005 68656c6c6f
        """
      When the broker decodes the message set
      Then the broker reads a record at offset 0 with no timestamp, a null key and the value hello

  Rule: A magic 1 message carries an 8-byte timestamp after the attributes

    Scenario: A magic 1 message carries its timestamp between attributes and key
      Given a record at offset 0 with a null key, the value hello and timestamp 1700000000000
      When a legacy producer encodes it with magic 1
      Then the message set bytes are
        """
        0000000000000000 0000001b 8ee30bba 01 00 0000018bcfe56800 ffffffff 00000005 68656c6c6f
        """

    Scenario: A magic 1 record without a timestamp is written with no timestamp marker -1
      Given a record at offset 0 with a null key and the value hello
      When a legacy producer encodes it with magic 1
      Then the message carries timestamp -1

  Rule: The CRC covers every message byte from magic through value

    Scenario: A message with an intact CRC decodes
      Given a message set from a legacy producer
        """
        0000000000000000 0000001b 8ee30bba 01 00 0000018bcfe56800 ffffffff 00000005 68656c6c6f
        """
      When the broker decodes the message set
      Then the broker reads a record at offset 0 with timestamp 1700000000000, a null key and the value hello

    Scenario Outline: A message with a changed byte under its CRC is rejected as corrupt
      Given a magic 1 message whose <part> has been changed after its CRC was computed
      When the broker decodes the message set
      Then the message set is rejected as corrupt

      Examples:
        | case                      | part      |
        | first covered byte        | magic     |
        | last covered byte         | value     |
        | the stored checksum       | crc       |

    Scenario: A changed entry offset does not break the CRC
      Given a magic 1 message whose offset has been changed after its CRC was computed
      When the broker decodes the message set
      Then the broker reads the record at the changed offset

  Rule: Only magic 0 and magic 1 are legacy messages

    Scenario: A message with magic 2 and a valid CRC is rejected as unsupported
      Given a message with magic 2 and a valid CRC
      When the broker decodes the message set
      Then the message set is rejected for unsupported magic 2

  Rule: A null key or value has length -1 and an empty one has length 0

    Scenario Outline: A key or value is written with the length that says whether it is null
      Given a record whose <field> is <content>
      When a legacy producer encodes it with magic 0
      Then the <field> length on the wire is <length>
      And the <field> reads back as <content>

      Examples:
        | case            | field | content | length |
        | null key        | key   | null    | -1     |
        | empty key       | key   | empty   | 0      |
        | tombstone value | value | null    | -1     |
        | empty value     | value | empty   | 0      |
