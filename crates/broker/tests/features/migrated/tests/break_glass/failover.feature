Feature: Approved proposals survive controller replacement

  Scenario: an approved proposal survives a controller failover
    Given a three-controller cluster holds approvals by bob,carol for deleting RF3 topic doomed
    When the controller that recorded the approvals shuts down and a survivor takes leadership
    Then a different controller holds the complete unchanged proposal and both approvals
    When the admin client retries deletion of the approved topic against the new controller
    Then deletion returns error 0 and the new controller records a nonzero consumption timestamp
