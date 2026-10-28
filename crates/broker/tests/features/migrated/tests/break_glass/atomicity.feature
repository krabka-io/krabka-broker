Feature: Break-glass consumption and authorized mutation commit atomically

  Scenario: the consume and the transition land in one raft append
    Given alice has bob and carol's approvals to unregister broker one
    When alice unregisters broker one and reads the newly committed controller batches
    Then unregistration succeeds and exactly one append contains consume first, only partition changes, and unregistration last
