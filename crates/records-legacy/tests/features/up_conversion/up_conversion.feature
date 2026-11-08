Feature: Legacy message sets up-convert to a v2 record batch

  A producer on Produce v0 or v1 sends magic 0 messages, and one on
  Produce v2 sends magic 1 messages. The broker up-converts the message
  set to one v2 record batch before it validates and appends it, with one
  v2 record per legacy message. The legacy layouts are in
  message_format/message_format.feature and
  compressed_wrappers/compressed_wrappers.feature.

  Rule: An up-converted batch is not idempotent

    Scenario: A legacy message set up-converts without a producer id, epoch and sequence
      Given a message set of magic 1 messages
      When the broker up-converts the message set
      Then the batch is not idempotent

  Rule: Record offsets become deltas from the first message offset

    Scenario: Offsets left apart by compaction keep their distance
      Given a message set of magic 1 messages at offsets 100, 102 and 105
      When the broker up-converts the message set
      Then the batch has base offset 100 and offset deltas 0, 2 and 5

  Rule: The batch timestamps span the earliest and the latest message timestamp

    Scenario: Out-of-order timestamps set the base and the max timestamp
      Given a message set of magic 1 messages with timestamps 2000, 1000 and 3000
      When the broker up-converts the message set
      Then the batch has base timestamp 1000, max timestamp 3000 and timestamp deltas 1000, 0 and 2000

    Scenario: Magic 0 messages up-convert without timestamps
      Given a message set of magic 0 messages
      When the broker up-converts the message set
      Then the batch has base timestamp -1 and max timestamp -1
