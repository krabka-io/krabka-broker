Feature: Static consumer membership survives restarts and fences competing clients

  Scenario: static rejoin preserves assignment and generation
    Given a stable group g-static-1 with static instance instance-A assigned assignment-bytes
    When the incumbent member heartbeats with the static instance ID
    Then the static heartbeat returns error 0
    When the static instance rejoins with an empty member ID
    Then the replacement keeps the generation and becomes leader without reassignment
    When the incumbent member heartbeats with the static instance ID
    Then the static heartbeat returns error 82
    When the replacement member requests its existing assignment
    Then the replacement receives the incumbent's assignment successfully

  Scenario: second client with same instance id is fenced
    Given a stable group g-fence with static instance instance-B assigned asgn
    When a member named imposter-member-id joins the incumbent static instance
    Then the intruder join is fenced with FENCED_INSTANCE_ID
    When the wrong member heartbeats with the static instance ID
    Then the static heartbeat returns error 82
    When the incumbent member heartbeats with the static instance ID
    Then the static heartbeat returns error 0

  Scenario: leave group resolves static member by instance id
    Given a stable group g-leave with static instance instance-C assigned asgn
    When the static instance leaves with an empty member ID
    Then the leave response echoes the static instance and succeeds for its single member
    When the departed incumbent heartbeats without a static instance ID
    Then the departed incumbent's heartbeat is rejected
