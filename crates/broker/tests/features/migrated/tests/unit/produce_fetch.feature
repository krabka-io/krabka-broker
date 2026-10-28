Feature: Kafka record offsets and fetch round trips
  Scenario: produce assigns base offsets
    Given a single broker for record requests
    And an empty single-partition topic named prod
    When the client produces 3 records to the topic
    Then the latest produce response contains one successful partition at offset 0
    When the client produces 2 records to the topic
    Then the latest produce response contains one successful partition at offset 3

  Scenario: produce without a topic id returns unknown topic id
    Given a single broker for record requests
    When the client produces without a topic id
    Then produce without a topic id returns UNKNOWN_TOPIC_ID

  Scenario: produce then fetch round trip
    Given a single broker for record requests
    And an empty single-partition topic named round
    When the client produces 3 records to the topic
    Then the latest produce response contains one successful partition at offset 0
    When the client fetches the topic from offset zero
    Then fetch returns one topic containing 3 records

  Scenario: list offsets earliest and latest
    Given a single broker for record requests
    And an empty single-partition topic named empty
    When the client requests the earliest and latest offsets
    Then both offset bounds are zero without a Kafka error
