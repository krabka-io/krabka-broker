# Quoted <empty> denotes a zero-length Kafka string; an empty created-topic list contains no names.
Feature: Metadata authorization, automatic creation and ordered topic rows

  Scenario: cluster Create auto-creates a missing topic and answers 3
    Given a metadata broker with automatic topic creation true and protocol version 12
    And the metadata principal holds these exact grants
      | resource | operation |
      | authz-cluster-create | Describe |
      | cluster | Create |
    When metadata requests automatic creation true and authorized operations false for these topics
      | name |
      | authz-cluster-create |
    Then metadata returns these complete ordered topic rows
      | name | result | operations |
      | authz-cluster-create | 3 | -2147483648 |
    And only these requested missing topics are created: "authz-cluster-create"

  Scenario: topic Create alone auto-creates a missing topic
    Given a metadata broker with automatic topic creation true and protocol version 12
    And the metadata principal holds these exact grants
      | resource | operation |
      | authz-topic-create | Describe |
      | authz-topic-create | Create |
    When metadata requests automatic creation true and authorized operations false for these topics
      | name |
      | authz-topic-create |
    Then metadata returns these complete ordered topic rows
      | name | result | operations |
      | authz-topic-create | 3 | -2147483648 |
    And only these requested missing topics are created: "authz-topic-create"

  Scenario: no Create answers 29 with the zero id
    Given a metadata broker with automatic topic creation true and protocol version 12
    And the metadata principal holds these exact grants
      | resource | operation |
      | authz-no-create | Describe |
    When metadata requests automatic creation true and authorized operations false for these topics
      | name |
      | authz-no-create |
    Then metadata returns these complete ordered topic rows
      | name | result | operations |
      | authz-no-create | 29 | -2147483648 |
    And only these requested missing topics are created: "<empty>"

  Scenario: no auto-creation asked needs no Create
    Given a metadata broker with automatic topic creation true and protocol version 12
    And the metadata principal holds these exact grants
      | resource | operation |
      | authz-no-auto | Describe |
    When metadata requests automatic creation false and authorized operations false for these topics
      | name |
      | authz-no-auto |
    Then metadata returns these complete ordered topic rows
      | name | result | operations |
      | authz-no-auto | 3 | -2147483648 |
    And only these requested missing topics are created: "<empty>"

  Scenario: an invalid name answers 17 and is not created
    Given a metadata broker with automatic topic creation true and protocol version 12
    And the metadata principal holds these exact grants
      | resource | operation |
      | authz bad name | Describe |
      | cluster | Create |
    When metadata requests automatic creation true and authorized operations false for these topics
      | name |
      | authz bad name |
    Then metadata returns these complete ordered topic rows
      | name | result | operations |
      | authz bad name | 17 | -2147483648 |
    And only these requested missing topics are created: "<empty>"

  Scenario: rows are described, then denied Create, then denied Describe
    Given a metadata broker with automatic topic creation true and protocol version 12
    And the metadata principal holds these exact grants
      | resource | operation |
      | authz-existing | Describe |
      | authz-order-no-create | Describe |
    When metadata requests automatic creation true and authorized operations false for these topics
      | name |
      | authz-hidden |
      | authz-order-no-create |
      | authz-existing |
    Then metadata returns these complete ordered topic rows
      | name | result | operations |
      | authz-existing | full | -2147483648 |
      | authz-order-no-create | 29 | -2147483648 |
      | authz-hidden | 29 | -2147483648 |
    And only these requested missing topics are created: "<empty>"

  Scenario: a repeated name answers one row
    Given a metadata broker with automatic topic creation true and protocol version 12
    And the metadata principal holds these exact grants
      | resource | operation |
      | authz-existing | Describe |
      | authz-twice | Describe |
    When metadata requests automatic creation false and authorized operations false for these topics
      | name |
      | authz-existing |
      | authz-existing |
      | authz-twice |
      | authz-twice |
    Then metadata returns these complete ordered topic rows
      | name | result | operations |
      | authz-existing | full | -2147483648 |
      | authz-twice | 3 | -2147483648 |
    And only these requested missing topics are created: "<empty>"

  Scenario: the missing topic row carries its authorized operations
    Given a metadata broker with automatic topic creation true and protocol version 10
    And the metadata principal holds these exact grants
      | resource | operation |
      | authz-existing | Describe |
      | authz-ops-missing | Describe |
      | authz-ops-missing | Create |
    When metadata requests automatic creation false and authorized operations true for these topics
      | name |
      | authz-ops-missing |
      | authz-existing |
    Then metadata returns these complete ordered topic rows
      | name | result | operations |
      | authz-existing | full | 256 |
      | authz-ops-missing | 3 | 288 |
    And only these requested missing topics are created: "<empty>"

  Scenario: auto.create.topics.enable = false answers 3 and creates nothing
    Given a metadata broker with automatic topic creation false and protocol version 12
    And the metadata principal holds these exact grants
      | resource | operation |
      | authz-auto-create-disabled | Describe |
      | cluster | Create |
    When metadata requests automatic creation true and authorized operations false for these topics
      | name |
      | authz-auto-create-disabled |
    Then metadata returns these complete ordered topic rows
      | name | result | operations |
      | authz-auto-create-disabled | 3 | -2147483648 |
    And only these requested missing topics are created: "<empty>"
