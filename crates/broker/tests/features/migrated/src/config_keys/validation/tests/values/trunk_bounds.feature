Feature: Trunk topic keys retain Kafka value validation after opt-in
  Opt-in enables names, not invalid lags, batch sizes, or boolean encodings.

  Scenario Outline: Trunk topic keys retain Kafka value validation after opt-in
    Given a topic alteration supplies key <key> and quoted value <value>
    When the supplied trunk topic value is canonicalized with unstable APIs enabled
    Then the trunk topic value is rejected by its validator

    Examples:
      | key | value |
      | remote.copy.lag.ms | "-2" |
      | remote.copy.lag.bytes | "-2" |
      | max.decompressed.message.bytes | "0" |
      | max.decompressed.message.bytes | "2147483640" |
      | errors.deadletterqueue.group.enable | "yes" |
