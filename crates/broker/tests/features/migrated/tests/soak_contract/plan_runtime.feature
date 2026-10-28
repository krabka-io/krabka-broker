Feature: Soak node configuration applies the contract's cluster requirements
  Scenario: tests: the node file carries the contracts cluster limits
    Given soak contract documents self-check.yaml,rf3-72h.yaml are loaded
    When the executor writes and applies the contract node configuration
    Then each node configuration retains its contract's timeouts replication and minimum ISR

  Scenario Outline: tests: an object store contract shares one local store
    Given the self-check contract adds an idempotent <storage> path
    When the executor writes and applies the contract node configuration
    Then object-store paths share the configured local tier with a three-replica topic-backed RLMM

    Examples:
      | example  | storage  |
      | tiered   | tiered   |
      | diskless | diskless |

  Scenario: tests: a diskless contract votes every broker into the wal
    Given soak contract documents rf3-72h.yaml are loaded
    When the executor writes and applies the contract node configuration
    Then every one of the three brokers is a local WAL replica
