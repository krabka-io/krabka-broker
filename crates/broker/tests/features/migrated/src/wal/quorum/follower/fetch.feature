Feature: src wal quorum follower fetch

  Scenario Outline: tests: diverging epoch is absent at the kafka default and needs a real epoch
    Then wal quorum follower fetch tests: diverging epoch is absent at the kafka default and needs a real epoch for <example>

    Examples:
      | example             |
      | kafka default       |
      | negative end offset |
      | named divergence    |
      | epoch zero          |
      | negative epoch      |
