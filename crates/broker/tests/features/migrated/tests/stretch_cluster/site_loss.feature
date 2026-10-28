Feature: tests stretch cluster site loss

  Scenario: preferred site holds replicas zero and the leader
    Then stretch cluster site loss: preferred site holds replicas zero and the leader

  Scenario: acks all survives the loss of the non preferred data site
    Then stretch cluster site loss: acks all survives the loss of the non preferred data site

  Scenario: a single site loss keeps acks all committing
    Then stretch cluster site loss: a single site loss keeps acks all committing

  Scenario: both data sites down leaves no leader and the witness refuses writes
    Then stretch cluster site loss: both data sites down leaves no leader and the witness refuses writes
