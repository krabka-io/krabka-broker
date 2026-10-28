Feature: Streams topology descriptions are requested once and cleared when groups are deleted

  Background:
    Given a trunk streams broker runs the in-memory topology description plugin with one source partition

  Scenario: one member is asked and its push is described
    When a new member joins the topology-description group
    And a new member joins the topology-description group
    Then both topology-description joins succeed and only the first member is asked to push
    When the administrator describes the group including its topology description
    Then the topology description has status one and no stored description
    When the selected first member pushes the source-node description at topology epoch zero
    Then the topology description push returns the complete successful default response
    When the administrator describes the group including its topology description
    Then the group description changes only to status three with the exact pushed source topology
    When both joined members heartbeat after the topology description push
    Then both later heartbeats succeed without requesting another topology push

  Scenario: a deleted and recreated group is asked again
    When a new member joins the topology-description group
    Then the latest join succeeds and requests a topology description
    When the selected first member pushes the source-node description at topology epoch zero
    Then the topology description push returns the complete successful default response
    When the topology-description group's member leaves permanently
    Then the topology-description leave succeeds
    When the empty topology-description group is deleted
    Then exactly one topology-description group deletion succeeds
    When a new member joins the topology-description group
    Then the latest join succeeds and requests a topology description
    When the administrator describes the group including its topology description
    Then the topology description has status one and no stored description
