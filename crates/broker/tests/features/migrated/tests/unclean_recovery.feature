Feature: tests unclean recovery

  Scenario: unclean recovery elects longest log replica
    Then unclean recovery: unclean recovery elects longest log replica

  Scenario: unclean recovery prefers an eligible leader replica
    Then unclean recovery: unclean recovery prefers an eligible leader replica

  Scenario: unclean recovery never elects a witness from the elr
    Then unclean recovery: unclean recovery never elects a witness from the elr
