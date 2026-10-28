Feature: Durable prepared transactions complete after interrupted marker fanout

  Scenario Outline: end txn cut between prepare and complete completes after restart
    Given transaction <transactional_id> contains a,b,c at feature level <level> with marker fanout held
    When the producer requests <outcome> and EndTxn reaches held marker fanout
    And the broker shuts down while its EndTxn waits for markers
    Then the held EndTxn connection closes without a response
    When the prepared transaction broker restarts from its durable directory
    And the producer retries the prepared transaction's EndTxn
    Then the latest prepared EndTxn succeeds with the next producer epoch
    When a committed reader reads the prepared transaction followed by a plain z
    Then the prepared transaction's committed values are <values>

    Examples:
      | transactional_id | level | outcome | values  |
      | cut-commit       | 2     | commit  | a,b,c,z |
      | cut-abort        | 2     | abort   | z       |
      | cut-commit-tv1   | 1     | commit  | a,b,c,z |

  Scenario: init producer id during prepare commit is concurrent transactions
    Given transaction init-during-prepare contains a,b,c at feature level 2 with marker fanout held
    When the producer requests commit and EndTxn reaches held marker fanout
    And InitProducerId probes the prepared transaction using no identity
    Then the prepared InitProducerId returns error 51 and no identity
    When InitProducerId probes the prepared transaction using the transaction identity
    Then the prepared InitProducerId returns error 51 and no identity
    When InitProducerId probes the prepared transaction using another producer ID
    Then the prepared InitProducerId returns error 90 and no identity
    When marker fanout is reopened
    And the held EndTxn finishes after marker fanout resumes
    Then the latest prepared EndTxn succeeds with the next producer epoch
    When a committed reader reads the prepared transaction followed by a plain z
    Then the prepared transaction's committed values are a,b,c,z

  Scenario: end txn with a failed marker fanout answers none and completes later
    Given transaction failed-fanout contains a,b,c at feature level 2 with marker fanout failing
    When the producer commits while marker fanout fails
    Then the latest prepared EndTxn succeeds with the next producer epoch
    When marker fanout is reopened
    And the producer retries the prepared transaction's EndTxn
    Then the latest prepared EndTxn succeeds with the next producer epoch
    When a committed reader reads the prepared transaction followed by a plain z
    Then the prepared transaction's committed values are a,b,c,z
