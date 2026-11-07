Feature: Operators freeze a topic or a topic namespace in one command

  These scenarios are what the operator asks for and what the tool reports.
  Which writes a freeze refuses and which reads it keeps is the broker's to
  specify, in crates/broker/tests/features/migrated/tests/topic_freeze.

  Rule: A freeze takes one operator and no approval

    Scenario: An operator freezes the payments topic on their own
      When alice freezes the payments topic
      Then the registry holds an unsigned freeze on the payments topic set by alice

    Scenario: An operator freezes every topic under a name prefix
      When alice freezes every topic starting with tenant-a.
      Then the registry holds an unsigned freeze on every topic starting with tenant-a. set by alice

  Rule: A scope that reaches an internal topic is an ordinary refusal

    Scenario: Freezing the internal topic namespace is refused
      When alice tries to freeze every topic starting with __
      Then the tool reports a refusal
      And the registry holds no freeze
