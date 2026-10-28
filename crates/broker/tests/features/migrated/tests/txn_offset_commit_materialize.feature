Feature: tests txn offset commit materialize

  Scenario: txn offset commit visible via offset fetch after commit marker
    Then txn offset commit materialize: txn offset commit visible via offset fetch after commit marker

  Scenario: txn offset commit dropped on abort marker
    Then txn offset commit materialize: txn offset commit dropped on abort marker

  Scenario Outline: require stable offset fetch tracks transaction outcome
    Then txn offset commit materialize: require stable offset fetch tracks transaction outcome for <example>

    Examples:
      | example |
      | commit  |
      | abort   |
