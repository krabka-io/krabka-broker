Feature: An unreachable live leader refuses unreplicated acknowledgements and recovers after healing

  Scenario: an unreachable leader stops acknowledging acks all and recovers on heal
    Given a live three-site RF3 partition has data leader 1 and an incoming relay in front of each site's listeners
    When acks all writes are retried against the live leader until a committed response
    Then the live leader returns NONE code 0
    When site 1's incoming relay is cut while every broker remains running
    And a direct client sends the live leader one acks all write with a 3000-millisecond timeout
    Then the stranded write is not acknowledged with NONE
    And the unreachable leader remains node 1 with ISR containing only 1
    When a direct client sends the live leader one acks all write with a 3000-millisecond timeout
    Then the live leader refuses the write with NOT_ENOUGH_REPLICAS code 19
    And witness 3 never becomes leader over three seconds
    When site 1's incoming relay heals
    Then leader 1's ISR returns to data replicas 1,2 and witness 3
    When acks all writes are retried against the live leader until a committed response
    Then the live leader returns NONE code 0
