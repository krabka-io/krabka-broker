# Quoted <empty> denotes a zero-length Kafka string; an empty created-topic list contains no names.
Feature: Share offset deletion refusal shape and group identity

  Scenario: Disabled API returns top-level unsupported version
    Given a share deletion broker with API enabled false and group access allowed true
    When DeleteShareGroupOffsets targets group "g1" and topics missing
    Then the complete share deletion response has code 35, message "The version of API is not supported." and these topic rows
      | topic | code | message |
    And share offset deletion creates no new share group

  Scenario: Denied group returns top-level authorization failure
    Given a share deletion broker with API enabled true and group access allowed false
    When DeleteShareGroupOffsets targets group "g1" and topics missing
    Then the complete share deletion response has code 30, message "Group authorization failed." and these topic rows
      | topic | code | message |
    And share offset deletion creates no new share group

  Scenario: An empty group ID is invalid
    Given a share deletion broker with API enabled true and group access allowed true
    And topics t and elsewhere exist, classic is classic, share-empty has no state and share-held holds elsewhere partition 0
    When DeleteShareGroupOffsets targets group "<empty>" and topics t
    Then the complete share deletion response has code 24, message "The group id is invalid." and these topic rows
      | topic | code | message |
    And share offset deletion creates no new share group

  Scenario: An absent group is not created
    Given a share deletion broker with API enabled true and group access allowed true
    And topics t and elsewhere exist, classic is classic, share-empty has no state and share-held holds elsewhere partition 0
    When DeleteShareGroupOffsets targets group "absent" and topics t
    Then the complete share deletion response has code 69, message "Group absent not found." and these topic rows
      | topic | code | message |
    And share offset deletion creates no new share group

  Scenario: A classic group cannot be deleted as share offsets
    Given a share deletion broker with API enabled true and group access allowed true
    And topics t and elsewhere exist, classic is classic, share-empty has no state and share-held holds elsewhere partition 0
    When DeleteShareGroupOffsets targets group "classic" and topics t
    Then the complete share deletion response has code 69, message "Group classic is not a share group." and these topic rows
      | topic | code | message |
    And share offset deletion creates no new share group

  Scenario: A share group without partition metadata returns no rows
    Given a share deletion broker with API enabled true and group access allowed true
    And topics t and elsewhere exist, classic is classic, share-empty has no state and share-held holds elsewhere partition 0
    When DeleteShareGroupOffsets targets group "share-empty" and topics t,missing-topic
    Then the complete share deletion response has code 0, message "null" and these topic rows
      | topic | code | message |
    And share offset deletion creates no new share group

  Scenario: Existing share metadata distinguishes unstored and unknown topics
    Given a share deletion broker with API enabled true and group access allowed true
    And topics t and elsewhere exist, classic is classic, share-empty has no state and share-held holds elsewhere partition 0
    When DeleteShareGroupOffsets targets group "share-held" and topics t,missing-topic
    Then the complete share deletion response has code 0, message "null" and these topic rows
      | topic | code | message |
      | t | 3 | There is no offset information to delete. |
      | missing-topic | 3 | This server does not host this topic-partition. |
    And share offset deletion creates no new share group
