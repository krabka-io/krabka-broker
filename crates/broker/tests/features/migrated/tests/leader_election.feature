Feature: tests leader election

  Scenario: broker death elects new leader
    Given a registered three-broker RF3 topic named elect has elected leadership and ISR
    When broker 1 shuts down
    And the survivors elect a controller and a partition leader and Metadata reads that partition
    Then the new partition leader is broker 2 or 3 and its leader epoch is greater than zero

  Scenario: unclean failover recovers after a real broker restart
    Given a registered three-broker RF3 topic named kip704-restart has elected leadership and ISR
    And unclean leader election is enabled and the current leader is the only ISR member
    When the sole ISR leader shuts down while a survivor observes Recovering
    Then the survivor reports Recovered with a leader different from the stopped broker
    When the stopped broker rejoins from its original log and addresses
    Then the restarted broker also reports Recovered

  Scenario: acks all completes after isr shrink
    Given a registered three-broker RF3 topic named shrink2 has elected leadership and ISR
    When broker 3 shuts down
    And acks all appends a,b,c with a 15000-millisecond timeout while ISR shrinks
    Then the append starts at offset 0 and completes in less than ten seconds

  Scenario: isr expand on catchup
    Given a registered three-broker RF3 topic named expand has elected leadership and ISR
    When the controller finalizes kraft.version 1
    Then the version activation is committed
    When the highest-numbered non-controller follower leaves the voter set, reserves a replacement client address and shuts down
    Then the remaining partition ISR has two members
    When the follower starts on an empty log through localhost KIP-853 auto-join, with the occupied client port released after one second
    Then the partition ISR expands back to three members

  Scenario: produce during leader failover
    Given a registered three-broker RF3 topic named failover has elected leadership and ISR
    When acks one writes a,b,c,d,e before the leader is lost
    And broker 1 shuts down
    And the survivors elect a controller and a partition leader and Metadata reads that partition
    Then the new partition leader is broker 2 or 3 and its leader epoch is greater than zero
    When acks one attempts f,g,h,i,j through the first surviving broker
    Then all five pre-loss writes were acknowledged and each post-loss attempt either succeeds or requests a leader refresh with code 6

  Scenario: a leader restarted inside its session is fenced and loses leadership
    Given three brokers use nine-second heartbeat sessions and an RF3 restart-fencing partition is led by a non-controller broker
    When acks all commits a,b,c,d,e and the observer records the leader epoch before restart
    And the partition leader crashes inside its existing heartbeat session
    And the leader restarts concurrently with failover and an acks all after record must commit within twenty-five seconds of the crash
    Then a surviving broker leads at a higher epoch and its local records are a,b,c,d,e then the acknowledged after
    And the restarted broker rejoins the ISR unfenced as a follower

  Scenario: a leader restarted inside its session hands leadership to the active controller
    Given three brokers use nine-second heartbeat sessions and an RF3 controller-successor partition is led by a non-controller broker whose next replica is the active controller
    When acks all commits a,b,c,d,e and the observer records the leader epoch before restart
    And the partition leader crashes inside its existing heartbeat session
    And the leader restarts concurrently with failover and an acks all after record must commit within twenty-five seconds of the crash
    Then a surviving broker leads at a higher epoch and its local records are a,b,c,d,e then the acknowledged after
    And the active controller leads the partition locally at the new epoch while it remains controller leader
    And the restarted broker rejoins the ISR unfenced as a follower

  Scenario: a controller and partition leader restarted inside its session hands leadership to the next controller
    Given three brokers use nine-second heartbeat sessions and the active controller leads one RF3 partition per survivor, with that survivor next in its replica order
    When the partition leader crashes inside its existing heartbeat session
    And a survivor takes over the metadata quorum
    And the stopped broker rejoins from its original log and addresses
    Then within twenty-five seconds the new active controller leads the partition it succeeds to at a higher epoch, serves acks all on it, and holds a,b,c,d,e then the acknowledged after

  Scenario: a controller leader restarted inside its session loses its partition leadership
    Given node 1 alone votes as controller and leads RF3 controller-restart-fencing with broker-only replicas 2 and 3 and nine-second heartbeat sessions
    When the partition leader crashes inside its existing heartbeat session
    And the stopped broker rejoins from its original log and addresses
    Then within twenty-five seconds partition leadership moves off the restarted controller at a higher epoch while it remains controller leader
    And the restarted controller rejoins the ISR unfenced as a follower within twenty-five seconds
