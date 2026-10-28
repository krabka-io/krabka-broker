Feature: OffsetCommit fences requests against Kafka group rules
  Scenario: Group kind membership generation and protocol version determine commit fencing: state=missing, version=9, member=empty (1)
    Given a ready offset coordinator and a group that is missing
    And an OffsetCommit version 9 request uses member empty and generation -1
    When the consumer commits its offset through the group coordinator
    Then the complete commit response has partition error 0 and group existence is true

  Scenario: Group kind membership generation and protocol version determine commit fencing: state=missing, version=9, member=m1 (2)
    Given a ready offset coordinator and a group that is missing
    And an OffsetCommit version 9 request uses member m1 and generation 3
    When the consumer commits its offset through the group coordinator
    Then the complete commit response has partition error 69 and group existence is false

  Scenario: Group kind membership generation and protocol version determine commit fencing: state=missing, version=8, member=m1 (3)
    Given a ready offset coordinator and a group that is missing
    And an OffsetCommit version 8 request uses member m1 and generation 3
    When the consumer commits its offset through the group coordinator
    Then the complete commit response has partition error 22 and group existence is false

  Scenario: Group kind membership generation and protocol version determine commit fencing: state=classic stable with member m1 at generation 3, version=8, member=empty (4)
    Given a ready offset coordinator and a group that is classic stable with member m1 at generation 3
    And an OffsetCommit version 8 request uses member empty and generation -1
    When the consumer commits its offset through the group coordinator
    Then the complete commit response has partition error 25 and group existence is true

  Scenario: Group kind membership generation and protocol version determine commit fencing: state=classic stable with member m1 at generation 3, version=9, member=empty (5)
    Given a ready offset coordinator and a group that is classic stable with member m1 at generation 3
    And an OffsetCommit version 9 request uses member empty and generation -1
    When the consumer commits its offset through the group coordinator
    Then the complete commit response has partition error 25 and group existence is true

  Scenario: Group kind membership generation and protocol version determine commit fencing: state=classic stable with member m1 at generation 3, version=9, member=m1 (6)
    Given a ready offset coordinator and a group that is classic stable with member m1 at generation 3
    And an OffsetCommit version 9 request uses member m1 and generation 3
    When the consumer commits its offset through the group coordinator
    Then the complete commit response has partition error 0 and group existence is true

  Scenario: Group kind membership generation and protocol version determine commit fencing: state=an empty consumer group, version=9, member=empty (7)
    Given a ready offset coordinator and a group that is an empty consumer group
    And an OffsetCommit version 9 request uses member empty and generation -1
    When the consumer commits its offset through the group coordinator
    Then the complete commit response has partition error 0 and group existence is true

  Scenario: Group kind membership generation and protocol version determine commit fencing: state=a streams group whose actor stopped, version=9, member=empty (8)
    Given a ready offset coordinator and a group that is a streams group whose actor stopped
    And an OffsetCommit version 9 request uses member empty and generation -1
    When the consumer commits its offset through the group coordinator
    Then the complete commit response has partition error 0 and group existence is true

