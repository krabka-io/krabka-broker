Feature: Single-voter controller topic creation
  A fresh controller elects its single voter, commits topic metadata, and refuses
  another creation of the same topic identity instead of silently duplicating it.

  Background:
    Given a fresh single-voter controller ready to create topic t

  Scenario: A single-voter leader commits a new topic
    When the single-voter controller is asked to create that topic
    Then the topic creation succeeds and appears in the controller metadata image

  Scenario: A single-voter leader refuses a duplicate topic
    Given that topic is already committed in the single-voter metadata log
    When the single-voter controller is asked to create that topic
    Then the duplicate topic creation fails with TopicExists
