Feature: Every outcome reaches a runbook as one exit code

  A runbook branches on the exit code, and the README and KFC-9 fix what each
  one means: 0 accepted, 1 refused, 2 unreachable, 3 unverifiable, 4 approval
  missing, 5 signature failure and 64 a command line that does not parse.

  Rule: Each broker answer becomes one exit code

    Scenario Outline: A broker answer reaches the runbook as its exit code
      When the broker answers a request with <answer>
      Then the tool exits with code <code>

      Examples:
        | case                             | answer                         | code |
        | accepted                         | no error                       | 0    |
        | a second person is needed        | BREAK_GLASS_APPROVAL_REQUIRED  | 4    |
        | a signature that did not verify  | OPERATOR_SIGNATURE_INVALID     | 5    |
        | a signature that was not sent    | OPERATOR_SIGNATURE_REQUIRED    | 5    |
        | an approver who already approved | BREAK_GLASS_DUPLICATE_APPROVER | 1    |
        | a Kafka authorization refusal    | CLUSTER_AUTHORIZATION_FAILED   | 1    |

  Rule: Each krabka-private refusal is explained in words

    Scenario Outline: The operator reads why the broker refused
      When the broker answers a request with <answer>
      Then the operator reads <explanation>

      Examples:
        | case                        | answer                         | explanation                                                             |
        | missing approval            | BREAK_GLASS_APPROVAL_REQUIRED  | error 1006 (no approved break-glass proposal covers this)               |
        | failed signature            | OPERATOR_SIGNATURE_INVALID     | error 1009 (the operator signature did not verify)                      |
        | full registry               | FREEZE_LIMIT_EXCEEDED          | error 1012 (the registry is at freeze.max_entries)                      |
        | proposer or repeat approver | BREAK_GLASS_DUPLICATE_APPROVER | error 1007 (this principal proposed the action or already approved it) |
        | a Kafka code it keeps       | CLUSTER_AUTHORIZATION_FAILED   | error 31                                                                |

  Rule: A broker nobody can reach is not a refusal

    Scenario: An operator freezes a topic while no broker answers
      Given no broker listens at the bootstrap address
      When alice tries to freeze the payments topic
      Then the tool reports an unreachable broker

  Rule: A command line the tool cannot parse is a usage error, not an unreachable broker

    Scenario: A flag the tool does not know ends on exit code 64
      When an operator runs the tool with a flag it does not know
      Then the tool reports a usage error

    Scenario: Asking the tool for help is not a usage error
      When an operator asks the tool for help
      Then the tool reports success
