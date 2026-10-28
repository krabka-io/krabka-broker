Feature: tests jbod disk failure

  Scenario: produce to partition on offline dir returns storage error
    Then jbod disk failure: produce to partition on offline dir returns storage error

  Scenario: all log dirs offline triggers self shutdown
    Then jbod disk failure: all log dirs offline triggers self shutdown

  Scenario: assign replicas to dirs reports and echoes
    Then jbod disk failure: assign replicas to dirs reports and echoes

  Scenario: heartbeat with offline log dirs is accepted
    Then jbod disk failure: heartbeat with offline log dirs is accepted
