Feature: Soak transitions preserve ordered placement and cumulative certificate trust
  Scenario Outline: tests: a reassignment drains the target onto the spare node
    Given partition t-0 has leader <leader> and replicas <replicas>
    When the executor plans draining broker four onto one of four nodes
    Then the partition replica plan is <planned>

    Examples:
      | example                 | leader | replicas | planned |
      | follower moved in place | 1      | 1,4,3    | 1,2,3   |
      | leader moved in place   | 4      | 4,2,3    | 1,2,3   |
      | not held                | 1      | 1,2,3    | none    |
      | nowhere to go           | 1      | 1,2,3,4  | none    |

  Scenario Outline: tests: a partition is on its plan in the planned order
    Given partition t-0 has leader 1 and replicas <replicas>
    And the ordered plan for t-0 is replicas one two and four
    When the executor checks whether replicas match the planned order
    Then the ordered replica plan has converged: <converged>

    Examples:
      | example       | replicas | converged |
      | same order    | 1,2,4    | true      |
      | another order | 4,1,2    | false     |
      | not yet       | 1,2,3    | false     |

  Scenario: tests: each issue comes from a new ca and trusts every earlier one
    Given no certificate authorities have yet issued node certificates
    When the executor issues two successive certificate generations
    Then the second generation trusts both CAs preserves the earlier trust and uses a new certificate
