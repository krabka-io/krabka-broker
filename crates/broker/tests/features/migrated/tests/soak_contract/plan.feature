Feature: tests soak contract plan

  Scenario Outline: tests: every checked in contract is runnable
    Then soak contract plan tests: every checked in contract is runnable for <example>

    Examples:
      | example    |
      | self check |
      | soak       |

  Scenario: tests: a drill is left to the qualification drill
    Then soak contract plan tests: a drill is left to the qualification drill

  Scenario: tests: a compacted tiered path is refused
    Then soak contract plan tests: a compacted tiered path is refused

  Scenario: tests: the node file carries the contracts cluster limits
    Then soak contract plan tests: the node file carries the contracts cluster limits

  Scenario Outline: tests: an object store contract shares one local store
    Then soak contract plan tests: an object store contract shares one local store for <example>

    Examples:
      | example  |
      | tiered   |
      | diskless |

  Scenario Outline: tests: only a diskless contract runs a wal quorum
    Then soak contract plan tests: only a diskless contract runs a wal quorum for <example>

    Examples:
      | example |
      | classic |
      | soak    |

  Scenario: tests: a diskless contract votes every broker into the wal
    Then soak contract plan tests: a diskless contract votes every broker into the wal

  Scenario Outline: tests: each path gets the topic its semantics need
    Then soak contract plan tests: each path gets the topic its semantics need for <example>

    Examples:
      | example    |
      | idempotent |
      | compacted  |
      | tiered     |
      | diskless   |
