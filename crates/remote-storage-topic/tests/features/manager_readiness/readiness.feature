Feature: Remote metadata readiness
  Scenario Outline: Readiness reflects assignment and current high water mark
    Given the metadata manager is <state>
    When the manager is asked whether its metadata is caught up
    Then the readiness result is <answer>

    Examples:
      | state                                        | answer                               |
      | caught up                                    | caught up                            |
      | unassigned                                   | behind                               |
      | awaiting the assignment high water mark       | not ready for the assigned partition |
      | unable to fetch the current high water mark   | an I/O error                         |
      | shut down                                    | an I/O error                         |

  Scenario: Committed peer writes make a previously caught up manager fall behind
    Given the metadata manager is caught up
    When a peer commits an event while the metadata pump is stalled and readiness is queried
    Then the readiness result is behind
