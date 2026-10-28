Feature: One site loss preserves committed writes and never promotes the witness

  Background:
    Given a three-site RF3 topic places data brokers 1 and 2 with witness 3 and min.insync.replicas 2

  Scenario: preferred site holds replicas zero and the leader
    Then broker 1 reports leader 1, replicas 1,2,3, ISR 1,2,3 and no adding or removing replicas
    When acks all writes are retried against broker 1 until a committed response
    Then the write returns NONE code 0

  Scenario: acks all survives the loss of the non preferred data site
    When site broker 2 shuts down
    Then broker 1 reports leader 1, replicas 1,2,3, ISR 1,3 and no adding or removing replicas
    When acks all writes are retried against broker 1 until a committed response
    Then the write returns NONE code 0

  Scenario Outline: a single site loss keeps acks all committing
    When site broker <stop> shuts down
    Then broker <observer> reports leader <leader>, replicas 1,2,3, ISR <isr> and no adding or removing replicas
    When acks all writes are retried against broker <observer> until a committed response
    Then the write returns NONE code 0

    Examples:
      | stop | observer | leader | isr |
      | 3    | 1        | 1      | 1,2 |
      | 1    | 2        | 2      | 2,3 |

  Scenario: both data sites down leaves no leader and the witness refuses writes
    When site broker 1 shuts down
    And site broker 2 shuts down
    Then witness 3 never becomes leader over five seconds
    When a client sends an acks all write directly to witness 3 with a 3000-millisecond timeout
    Then the write returns NOT_LEADER_OR_FOLLOWER code 6 and the partition view never names witness 3 as leader
