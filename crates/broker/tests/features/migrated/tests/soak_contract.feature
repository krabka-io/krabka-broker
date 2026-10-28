Feature: Declared sustained-load and lifecycle objectives hold against running brokers

  Scenario: the sustained load contract holds
    Given KRABKA_SOAK_CONTRACT or bench/soak/self-check.yaml declares a valid contract supported by this executor
    And the contract's broker, storage, security and lifecycle topology is running with its topics and full ISRs
    When the declared load runs for its full duration while scheduled faults, recoveries and lifecycle checkpoints are recorded
    And transactions settle, the self-check injects its declared loss, every path reads back and final commits and telemetry are saved
    Then the contract judge accepts all declared path, recovery, lifecycle and self-check objectives and writes verdict.json

  Scenario: an unsupported rollback is refused before it damages data
    Given bench/soak/lifecycle.yaml declares the source release for an unsupported rollback
    When the candidate finalizes metadata.version 4.4-IV2, the source release attempts rollback and the candidate reopens its record
    Then the refused rollback changes no durable file and the candidate reads the original record back
