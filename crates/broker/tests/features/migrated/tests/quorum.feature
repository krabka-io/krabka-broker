Feature: Metadata quorum elections and committed changes

  Rule: Every voter observes one surviving controller
    Scenario: Three metadata voters elect one controller
      Given three brokers share a static metadata voter set
      When all metadata voters observe their elected controller
      Then every surviving voter reports the same surviving nonzero controller

    Scenario: A controller shutdown elects a surviving replacement
      Given a three-broker metadata quorum has elected a controller
      When the elected metadata controller is shut down
      Then every surviving voter reports the same surviving nonzero controller

  Rule: A committed topic is visible through another metadata voter
    Scenario: Topic creation through a voter succeeds
      Given a three-broker metadata quorum has elected a controller
      When a client creates topic prop with 3 partitions through voter index 0
      Then the latest topic creation succeeds

    Scenario: Committed topic metadata reaches another voter
      Given a three-broker metadata quorum has elected a controller
      And topic prop has been created with three partitions through a metadata voter
      When a client reads metadata through voter index 2 after prop reaches its image
      Then voter index 2 reports topic prop in client metadata

  Rule: A replacement controller accepts metadata changes
    Scenario: Topic creation succeeds after a controller shutdown
      Given the metadata quorum has recovered from a controller shutdown
      When a client creates topic post-kill with 1 partitions through voter index 0
      Then the latest topic creation succeeds

  Rule: A follower forwards topic creation to the controller
    Scenario: Topic creation through a metadata follower succeeds
      Given a three-broker metadata quorum has elected a controller
      When a client creates topic via-follower through a metadata follower
      Then the latest topic creation succeeds
