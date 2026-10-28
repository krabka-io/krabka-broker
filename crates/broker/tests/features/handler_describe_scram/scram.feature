Feature: DescribeUserScramCredentials ordering and authorization responses
  Scenario: Credential ordering follows mechanism type rather than insertion order
    Given alice has SHA-512 with 8192 iterations before SHA-256 with 4096 iterations
    When alice's SCRAM credentials are described
    Then alice's credential mechanisms appear in Kafka order 1 then 2

  Scenario Outline: Authorization failure preserves request row count including duplicate users
    Given a denied SCRAM description request has user filter <users>
    When the denied SCRAM description response is constructed
    Then the denied response has <rows> anonymous credential-free error rows under cluster authorization failure
    Examples:
      | users           | rows |
      | null            | 0    |
      | empty           | 0    |
      | alice,bob,alice | 3    |
