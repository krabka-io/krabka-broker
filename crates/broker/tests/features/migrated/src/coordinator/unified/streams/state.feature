Feature: Streams member reconciliation uses explicit task ownership and target epochs

  Rule: A member moves to its target epoch only once it owns no task it must revoke or wait for

    Scenario: a stable member takes the new tasks at the target epoch
      Given Streams group g at epoch 2 has these members in subtopology s
        | member | process | state | epoch | active | standby | active_pending | standby_pending |
        | m1 | p1 | Stable | 1 | [0] | [] | [] | [] |
      And m1 target active tasks are [0,1] standby tasks [] and heartbeat owns active [0] standby []
      When the coordinator reconciles m1 against its target epoch 2 and heartbeat owned tasks
      Then m1 has this complete reconciliation result
        | changed | state | epoch | active | standby | active_pending | standby_pending |
        | true | Stable | 2 | [0,1] | [] | [] | [] |

    Scenario: a member keeps its epoch while it owns a revoked task
      Given Streams group g at epoch 2 has these members in subtopology s
        | member | process | state | epoch | active | standby | active_pending | standby_pending |
        | m1 | p1 | Stable | 1 | [0,1] | [] | [] | [] |
      And m1 target active tasks are [0] standby tasks [] and heartbeat owns active [0,1] standby []
      When the coordinator reconciles m1 against its target epoch 2 and heartbeat owned tasks
      Then m1 has this complete reconciliation result
        | changed | state | epoch | active | standby | active_pending | standby_pending |
        | true | UnrevokedTasks | 1 | [0] | [] | [1] | [] |

    Scenario: a member that reports no tasks has nothing to revoke
      Given Streams group g at epoch 2 has these members in subtopology s
        | member | process | state | epoch | active | standby | active_pending | standby_pending |
        | m1 | p1 | Stable | 1 | [0,1] | [] | [] | [] |
      And m1 target active tasks are [0] standby tasks [] and heartbeat owns active [] standby []
      When the coordinator reconciles m1 against its target epoch 2 and heartbeat owned tasks
      Then m1 has this complete reconciliation result
        | changed | state | epoch | active | standby | active_pending | standby_pending |
        | true | Stable | 2 | [0] | [] | [] | [] |

    Scenario: an unrevoked member moves on once it stops reporting the task
      Given Streams group g at epoch 2 has these members in subtopology s
        | member | process | state | epoch | active | standby | active_pending | standby_pending |
        | m1 | p1 | UnrevokedTasks | 1 | [0] | [] | [1] | [] |
      And m1 target active tasks are [0] standby tasks [] and heartbeat owns active [0] standby []
      When the coordinator reconciles m1 against its target epoch 2 and heartbeat owned tasks
      Then m1 has this complete reconciliation result
        | changed | state | epoch | active | standby | active_pending | standby_pending |
        | true | Stable | 2 | [0] | [] | [] | [] |

    Scenario: an unrevoked member that still reports the task waits
      Given Streams group g at epoch 2 has these members in subtopology s
        | member | process | state | epoch | active | standby | active_pending | standby_pending |
        | m1 | p1 | UnrevokedTasks | 1 | [0] | [] | [1] | [] |
      And m1 target active tasks are [0] standby tasks [] and heartbeat owns active [0,1] standby []
      When the coordinator reconciles m1 against its target epoch 2 and heartbeat owned tasks
      Then m1 has this complete reconciliation result
        | changed | state | epoch | active | standby | active_pending | standby_pending |
        | false | UnrevokedTasks | 1 | [0] | [] | [1] | [] |

    Scenario: an unrevoked member without owned tasks waits
      Given Streams group g at epoch 2 has these members in subtopology s
        | member | process | state | epoch | active | standby | active_pending | standby_pending |
        | m1 | p1 | UnrevokedTasks | 1 | [0] | [] | [1] | [] |
      And m1 target active tasks are [0] standby tasks [] and heartbeat owns active null standby null
      When the coordinator reconciles m1 against its target epoch 2 and heartbeat owned tasks
      Then m1 has this complete reconciliation result
        | changed | state | epoch | active | standby | active_pending | standby_pending |
        | false | UnrevokedTasks | 1 | [0] | [] | [1] | [] |

    Scenario: an active task that another member still owns is held back
      Given Streams group g at epoch 2 has these members in subtopology s
        | member | process | state | epoch | active | standby | active_pending | standby_pending |
        | m1 | p1 | Stable | 1 | [] | [] | [] | [] |
        | m2 | p2 | UnrevokedTasks | 1 | [] | [] | [1] | [] |
      And m1 target active tasks are [0,1] standby tasks [] and heartbeat owns active [] standby []
      When the coordinator reconciles m1 against its target epoch 2 and heartbeat owned tasks
      Then m1 has this complete reconciliation result
        | changed | state | epoch | active | standby | active_pending | standby_pending |
        | true | UnreleasedTasks | 2 | [0] | [] | [] | [] |

    Scenario: a standby task that the same process runs as active is held back
      Given Streams group g at epoch 2 has these members in subtopology s
        | member | process | state | epoch | active | standby | active_pending | standby_pending |
        | m1 | p1 | Stable | 1 | [] | [] | [] | [] |
        | m2 | p1 | Stable | 1 | [0] | [] | [] | [] |
      And m1 target active tasks are [] standby tasks [0] and heartbeat owns active [] standby []
      When the coordinator reconciles m1 against its target epoch 2 and heartbeat owned tasks
      Then m1 has this complete reconciliation result
        | changed | state | epoch | active | standby | active_pending | standby_pending |
        | true | UnreleasedTasks | 2 | [] | [] | [] | [] |

    Scenario: a standby task that another process runs as active is given
      Given Streams group g at epoch 2 has these members in subtopology s
        | member | process | state | epoch | active | standby | active_pending | standby_pending |
        | m1 | p1 | Stable | 1 | [] | [] | [] | [] |
        | m2 | p2 | Stable | 1 | [0] | [] | [] | [] |
      And m1 target active tasks are [] standby tasks [0] and heartbeat owns active [] standby []
      When the coordinator reconciles m1 against its target epoch 2 and heartbeat owned tasks
      Then m1 has this complete reconciliation result
        | changed | state | epoch | active | standby | active_pending | standby_pending |
        | true | Stable | 2 | [] | [0] | [] | [] |

    Scenario: a revoked standby task waits for its release
      Given Streams group g at epoch 2 has these members in subtopology s
        | member | process | state | epoch | active | standby | active_pending | standby_pending |
        | m1 | p1 | Stable | 1 | [] | [0] | [] | [] |
      And m1 target active tasks are [] standby tasks [] and heartbeat owns active [] standby [0]
      When the coordinator reconciles m1 against its target epoch 2 and heartbeat owned tasks
      Then m1 has this complete reconciliation result
        | changed | state | epoch | active | standby | active_pending | standby_pending |
        | true | UnrevokedTasks | 1 | [] | [] | [] | [0] |

  Rule: A member still revoking tasks at its rebalance timeout is fenced

    Scenario Outline: The rebalance timeout fences only a member that did not revoke in time
      Given member m1 with a rebalance timeout of 100 ms has been asked to revoke a task
      And the member <revocation>
      When the coordinator checks rebalance timeouts <checks> ms after the revocation began
      Then the checks fence <fenced>

      Examples:
        | case                                             | revocation                                        | checks | fenced  |
        | an early check keeps the deadline armed          | still owns the task                               | 99,100 | none,m1 |
        | revoked before the timeout                       | has revoked the task                              | 100    | none    |
        | a deadline armed at an older epoch does not fire | has moved to a newer epoch without a new deadline | 100    | none    |
        | leaving the revoking state cancels the deadline  | has left the revoking state at the same epoch     | 100    | none    |
