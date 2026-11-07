Feature: The tool finds the broker on the command line or in the environment
  An operator names the broker with --bootstrap-server, or exports
  KRABKA_BOOTSTRAP_SERVER once for a shell and leaves the flag out. The flag
  wins when both name one, and a command line that names no broker at all is
  refused before anything is sent. krabka-barrier finds its broker the same way:
  crates/barrier-cli/tests/features/broker_address/broker_address.feature.

  Rule: KRABKA_BOOTSTRAP_SERVER names the broker when the command line does not

    Scenario: An operator who names the broker only in the environment reaches it
      Given KRABKA_BOOTSTRAP_SERVER names the broker
      When an operator lists the freezes without --bootstrap-server
      Then the tool reports success

  Rule: --bootstrap-server wins over KRABKA_BOOTSTRAP_SERVER

    Scenario: The flag reaches the broker while the environment names an address where no broker listens
      Given KRABKA_BOOTSTRAP_SERVER names an address where no broker listens
      When an operator lists the freezes with --bootstrap-server naming the broker
      Then the tool reports success

  Rule: A command line that names no broker is refused

    Scenario: An operator who names no broker anywhere is refused before anything is sent
      Given KRABKA_BOOTSTRAP_SERVER is not set
      When an operator tries to list the freezes without --bootstrap-server
      Then the tool reports a refused command line
