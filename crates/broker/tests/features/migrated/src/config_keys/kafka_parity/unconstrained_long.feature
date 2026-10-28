Feature: Kafka LONG topic values without an invented lower bound
  retention.bytes has no range validator in Kafka: every representable LONG,
  including minus two, survives a MirrorMaker configuration replay.

  Scenario Outline: Unconstrained retention bytes accepts the complete LONG domain
    Given an operator alters topic key retention.bytes to quoted value <value>
    When the released topic alteration validator checks the operator value
    Then the individual topic alteration is accepted

    Examples:
      | value |
      | "-9223372036854775808" |
      | "-2" |
      | "-1" |
      | "0" |
      | "9223372036854775807" |
