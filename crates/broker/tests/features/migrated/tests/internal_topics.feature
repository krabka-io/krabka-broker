Feature: tests internal topics

  Scenario: metadata marks every broker owned topic internal
    Then internal topics: metadata marks every broker owned topic internal

  Scenario: describe topic partitions agrees with metadata
    Then internal topics: describe topic partitions agrees with metadata

  Scenario: a renamed audit topic is the internal one
    Then internal topics: a renamed audit topic is the internal one

  Scenario: a broker refuses an audit topic outside the convention
    Then internal topics: a broker refuses an audit topic outside the convention
