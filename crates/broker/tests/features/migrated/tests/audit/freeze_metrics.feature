Feature: Signed freeze and two person thaw workflow drives operator metrics
  Scenario: the kfc9 gauges and counters move on real requests
    Given alice bob and carol authenticate separately to a broker with signed freeze and two person approval policy
    Then the active freeze gauge is initially zero
    When alice signs and submits a freeze for orders
    Then the freeze succeeds and the active freeze gauge reaches one
    When alice proposes thawing orders
    Then the proposal succeeds and the pending proposal gauge reaches one
    When bob and carol independently approve the thaw proposal
    Then both approvals succeed and the proposal moves from pending to approved
    When alice signs and submits the thaw using the approved proposal
    Then the thaw succeeds and clears the freeze gauge while consuming the proposal
    When alice creates doomed without a deletion approval
    Then doomed is created successfully
    When alice attempts to delete doomed without approval
    Then deletion is refused by policy and increments the refusal counter once with an audit record
