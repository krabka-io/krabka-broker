Feature: Broker health and readiness probes
  Operators use liveness to keep recovering brokers alive and readiness to
  route traffic only to brokers that can serve it.

  Scenario: Liveness stays healthy while readiness changes at startup
    Given a broker whose log directories are still recovering
    When liveness and readiness are requested
    Then readiness is blocked by log_dir_recovery
    And liveness remains healthy
    When the broker finishes starting
    And liveness and readiness are requested
    Then every readiness response is ready
    And liveness remains healthy

  Scenario: Readiness names the first unfinished startup condition
    Given a broker whose log directories are still recovering
    When readiness is requested
    Then readiness is blocked by log_dir_recovery
    When log directory recovery completes
    And readiness is requested
    Then readiness is blocked by listeners_bound
    When the listeners have bound
    And readiness is requested
    Then readiness is blocked by metadata_quorum_unreached

  Scenario: A running broker stays ready across repeated probes
    Given a running broker with health probes
    When readiness is sampled three times
    Then every readiness response is ready

  Scenario: A ready broker leaves readiness when shutdown begins
    Given a running broker with health probes
    When readiness is requested
    Then every readiness response is ready
    When the broker begins shutting down
    And readiness is requested
    Then readiness is blocked by shutting_down

  Scenario: Shutdown takes precedence over unfinished startup
    Given a broker whose log directories are still recovering
    When readiness is requested
    Then readiness is blocked by log_dir_recovery
    When the broker begins shutting down
    And readiness is requested
    Then readiness is blocked by shutting_down
