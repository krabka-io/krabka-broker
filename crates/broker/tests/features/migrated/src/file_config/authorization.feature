Feature: File super-user lists accept Kafka principal names
  Kafka User prefixes are removed consistently for named and anonymous bypass users.

  Scenario: Top-level super users are normalized into local names
    Given a super-user configuration file contains:
      """
      super_users = ["User:admin", "User:ANONYMOUS", "operator"]
      """
    When the super-user file is applied and the broker configuration is validated
    Then the broker accepts super users admin, ANONYMOUS and operator as local principal names

  Scenario: Simple authorizer super users use the same Kafka normalization
    Given a super-user configuration file contains:
      """
      [authorization]
      type = "simple"
      super_users = ["User:admin", "User:ANONYMOUS", "operator"]
      """
    When the super-user file is applied and the broker configuration is validated
    Then the broker accepts super users admin, ANONYMOUS and operator as local principal names
