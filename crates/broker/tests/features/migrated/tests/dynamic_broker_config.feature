Feature: tests dynamic broker config

  Scenario: connection cap applies and lifts without restart
    Then dynamic broker config: connection cap applies and lifts without restart

  Scenario: per broker connection cap overrides the cluster default
    Then dynamic broker config: per broker connection cap overrides the cluster default

  Scenario: static connection cap holds without a dynamic value
    Then dynamic broker config: static connection cap holds without a dynamic value

  Scenario: invalid broker config changes are refused with kafkas errors
    Then dynamic broker config: invalid broker config changes are refused with kafkas errors

  Scenario: validate only broker change has no effect
    Then dynamic broker config: validate only broker change has no effect

  Scenario: describe reports dynamic values and redacts passwords
    Then dynamic broker config: describe reports dynamic values and redacts passwords

  Scenario: per ip cap applies to the controller listener
    Then dynamic broker config: per ip cap applies to the controller listener

  Scenario: zero per ip cap without overrides is stored but not applied
    Then dynamic broker config: zero per ip cap without overrides is stored but not applied

  Scenario: per ip overrides apply and lift without restart
    Then dynamic broker config: per ip overrides apply and lift without restart

  Scenario: listener cap applies and lifts without restart
    Then dynamic broker config: listener cap applies and lifts without restart

  Scenario: creation rate holds the acceptor without restart
    Then dynamic broker config: creation rate holds the acceptor without restart
