Feature: Leaderless partitions follow Kafka's last-known and eligible replica election ladder
  A last-known leader can return uncleanly only when eligible replicas do not
  supersede it. Witnesses, availability and recovery strategy constrain every rung.

  Scenario Outline: Choose a leader when <case>
    Given the recorded leader, replicas, ISR, ELR, live brokers and recovery policy are JSON <partition>
    When the controller elects a leader for the leaderless partition
    Then the complete leaderless election decision is JSON <decision>

    Examples:
      | case | partition | decision |
      | the last known leader returns | {"replicas":[1,2,3,4],"isr":[1],"eligible":[],"last_known":[1],"alive":[1,2,3,4],"witness_ids":[],"leader":1,"strategy":"None","unclean_enabled":false} | {"kind":"Elect","leader":1,"isr":[1],"unclean":true} |
      | with the unclean election on | {"replicas":[1,2,3,4],"isr":[1],"eligible":[],"last_known":[1],"alive":[1,2,3,4],"witness_ids":[],"leader":1,"strategy":"None","unclean_enabled":true} | {"kind":"Elect","leader":1,"isr":[1],"unclean":true} |
      | over balanced recovery | {"replicas":[1,2,3,4],"isr":[1],"eligible":[],"last_known":[1],"alive":[1,2,3,4],"witness_ids":[],"leader":1,"strategy":"Balanced","unclean_enabled":false} | {"kind":"Elect","leader":1,"isr":[1],"unclean":true} |
      | over aggressive recovery and the unclean election | {"replicas":[1,2,3,4],"isr":[1],"eligible":[],"last_known":[1],"alive":[1,2,3,4],"witness_ids":[],"leader":1,"strategy":"Aggressive","unclean_enabled":true} | {"kind":"Elect","leader":1,"isr":[1],"unclean":true} |
      | the last known leader wins over the first live replica | {"replicas":[2,3,1,4],"isr":[1],"eligible":[],"last_known":[1],"alive":[1,2,3,4],"witness_ids":[],"leader":1,"strategy":"None","unclean_enabled":true} | {"kind":"Elect","leader":1,"isr":[1],"unclean":true} |
      | a non-empty ELR shuts the last-known rung | {"replicas":[1,2,3,4],"isr":[1],"eligible":[3],"last_known":[1],"alive":[1,2,4],"witness_ids":[],"leader":1,"strategy":"None","unclean_enabled":false} | {"kind":"Unavailable"} |
      | an ELR member outranks the last known leader | {"replicas":[1,2,3,4],"isr":[1],"eligible":[3],"last_known":[1],"alive":[1,3],"witness_ids":[],"leader":1,"strategy":"None","unclean_enabled":false} | {"kind":"Elect","leader":3,"isr":[3],"unclean":false} |
      | the last known leader in the ELR is a clean election | {"replicas":[1,2,3,4],"isr":[1],"eligible":[1,2,3],"last_known":[1],"alive":[1,2,3,4],"witness_ids":[],"leader":1,"strategy":"None","unclean_enabled":false} | {"kind":"Elect","leader":1,"isr":[1],"unclean":false} |
      | nobody alive, unclean election off | {"replicas":[1,2,3,4],"isr":[1],"eligible":[],"last_known":[1],"alive":[],"witness_ids":[],"leader":1,"strategy":"None","unclean_enabled":false} | {"kind":"Unavailable"} |
      | nobody alive, unclean election on | {"replicas":[1,2,3,4],"isr":[1],"eligible":[],"last_known":[1],"alive":[],"witness_ids":[],"leader":1,"strategy":"None","unclean_enabled":true} | {"kind":"Unavailable"} |
      | the last known leader is down: KIP-841 is what is left | {"replicas":[1,2,3,4],"isr":[1],"eligible":[],"last_known":[1],"alive":[2,3],"witness_ids":[],"leader":1,"strategy":"None","unclean_enabled":true} | {"kind":"Elect","leader":2,"isr":[2],"unclean":true} |
      | the last known leader is down and the unclean election is off | {"replicas":[1,2,3,4],"isr":[1],"eligible":[],"last_known":[1],"alive":[2,3],"witness_ids":[],"leader":1,"strategy":"None","unclean_enabled":false} | {"kind":"Unavailable"} |
      | a witness is never the last known leader | {"replicas":[1,2,3,4],"isr":[1],"eligible":[],"last_known":[1],"alive":[1,2,3,4],"witness_ids":[1],"leader":1,"strategy":"None","unclean_enabled":false} | {"kind":"Unavailable"} |
      | a live ISR member leads before any out-of-ISR rung | {"replicas":[1,2,3,4],"isr":[1,2],"eligible":[],"last_known":[1],"alive":[1,2],"witness_ids":[],"leader":1,"strategy":"None","unclean_enabled":false} | {"kind":"Elect","leader":2,"isr":[2],"unclean":false} |
      | a multi-member last-known set is no marker | {"replicas":[1,2,3,4],"isr":[1],"eligible":[],"last_known":[1,2],"alive":[1,2,3,4],"witness_ids":[],"leader":1,"strategy":"None","unclean_enabled":false} | {"kind":"NoChange"} |
      | a partition that has a leader is not touched | {"replicas":[1,2,3,4],"isr":[1],"eligible":[],"last_known":[],"alive":[1,2,3,4],"witness_ids":[],"leader":1,"strategy":"None","unclean_enabled":false} | {"kind":"NoChange"} |
      | the marker names the recorded leader and nobody else | {"replicas":[1,2,3,4],"isr":[1],"eligible":[],"last_known":[2],"alive":[1,2,3,4],"witness_ids":[],"leader":1,"strategy":"None","unclean_enabled":false} | {"kind":"NoChange"} |
