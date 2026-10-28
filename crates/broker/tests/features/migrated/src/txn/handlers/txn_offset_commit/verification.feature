Feature: TxnOffsetCommit verification errors remain compatible with Kafka request versions

  Scenario Outline: Translate a transaction coordinator answer for the client
    Given TxnOffsetCommit version <version> received transaction coordinator code <coordinator>
    When the broker maps verification for the TxnOffsetCommit response
    Then the TxnOffsetCommit response verification code is <response>

    Examples:
      | coordinator | version | response |
      | 0 | 3 | 0 |
      | 0 | 5 | 0 |
      | 90 | 3 | 47 |
      | 90 | 5 | 47 |
      | 49 | 3 | 49 |
      | 51 | 5 | 51 |
      | 31 | 5 | 48 |
      | 120 | 3 | 48 |
      | 120 | 4 | 120 |
      | 120 | 5 | 120 |
      | 13 | 5 | 14 |
      | 3 | 5 | 15 |
      | 15 | 5 | 15 |
      | 6 | 5 | 16 |
