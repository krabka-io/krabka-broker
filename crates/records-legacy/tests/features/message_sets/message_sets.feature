Feature: A legacy message set is a run of offset-framed messages

  A message set has no length or count of its own. Each entry is an 8-byte
  offset, a 4-byte message size, and that many bytes of message. A reader
  walks entries until the bytes run out. Apache Kafka's
  ByteBufferLogInputStream stops at an entry that the bytes do not hold in
  full, and rejects an entry whose size is below the 14-byte magic 0
  message overhead as corrupt. The message layout is in
  message_format/message_format.feature.

  Rule: A partial message at the end of a message set is dropped

    Scenario: A message set that ends on a message boundary yields every record
      Given a message set of three magic 1 messages
      When the broker decodes the message set
      Then the broker reads three records

    Scenario: A message set cut inside its last message yields the complete ones
      Given a message set of three magic 1 messages cut inside the third message
      When the broker decodes the message set
      Then the broker reads two records

    Scenario: A message set cut inside the offset and size of its last entry yields the complete ones
      Given a message set of three magic 1 messages cut inside the third entry header
      When the broker decodes the message set
      Then the broker reads two records

  Rule: An entry whose message size is below the message overhead is corrupt

    Scenario Outline: An entry with an impossible message size is rejected
      Given a message set whose only entry has message size <size>
      When the broker decodes the message set
      Then the message set is rejected as invalid

      Examples:
        | case     | size |
        | empty    | 0    |
        | negative | -1   |
