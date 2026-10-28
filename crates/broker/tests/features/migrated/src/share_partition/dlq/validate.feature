Feature: Share group dead-letter topics follow Kafka validation order
  Topic presence, opt-in, prefix and auto-creation settings determine whether
  dead-letter delivery can proceed and which configuration error is reported.
  Cluster defaults require the dlq. prefix and disable automatic topic creation.

  Background:
    Given share group g has no configured dead-letter topic

  Scenario: An absent configured name is refused
    When the share partition validates group g's dead-letter topic
    Then dead-letter configuration fails with Configured DLQ topic name in share group: g is empty.

  Scenario: Internal topics are refused before other naming rules
    Given share group g names dead-letter topic __consumer_offsets
    When the share partition validates group g's dead-letter topic
    Then dead-letter configuration fails with Configured DLQ topic name in share group: g cannot start with __, topic: __consumer_offsets.

  Scenario: Wrong prefix is refused before missing topic
    Given share group g names dead-letter topic orders.dead
    When the share partition validates group g's dead-letter topic
    Then dead-letter configuration fails with Configured DLQ topic name does not comply with the DLQ topic prefix in share group: g, topic: orders.dead, prefix: dlq.

  Scenario: Missing topic is refused without cluster auto-creation
    Given share group g names dead-letter topic dlq.g
    When the share partition validates group g's dead-letter topic
    Then dead-letter configuration fails with DLQ topic does not exist and auto create is disabled on cluster for share group: g, topic: dlq.g.

  Scenario: Missing topic can be created when the cluster permits it
    Given share group g names dead-letter topic dlq.g
    Given the cluster permits automatic dead-letter topic creation
    When the share partition validates group g's dead-letter topic
    Then dead-letter validation accepts topic state missing and creatable

  Scenario: Existing opted-in topic is ready for delivery
    Given share group g names dead-letter topic dlq.g
    Given topic dlq.g exists with dead-letter opt-in true
    When the share partition validates group g's dead-letter topic
    Then dead-letter validation accepts topic state exists

  Scenario: Internal name refusal takes precedence over wrong prefix
    Given share group g names dead-letter topic __x
    When the share partition validates group g's dead-letter topic
    Then dead-letter configuration fails with Configured DLQ topic name in share group: g cannot start with __, topic: __x.

  Scenario Outline: Existing topics require an explicit enabled dead-letter flag
    Given share group g names dead-letter topic dlq.g
    And topic dlq.g exists with dead-letter opt-in <flag>
    When the share partition validates group g's dead-letter topic
    Then dead-letter configuration fails with DLQ is not enabled on configured DLQ topic for share group: g, topic: dlq.g

    Examples:
      | flag |
      | false |
      | unset |
