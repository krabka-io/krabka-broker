Feature: Witness roles are observable and controlled by the controller

  Scenario: witness role is a read only broker config
    Given a three-site cluster registers broker 3 as its witness with preferred leader site A
    When broker 3 describes all of its broker configs without synonyms or documentation
    Then broker.witness is true, read-only boolean from the broker and the preferred site is a read-only cluster-default string
    When the operator tries to set broker.witness to false through IncrementalAlterConfigs
    Then the witness rejects the change with INVALID_CONFIG
