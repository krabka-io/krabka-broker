Feature: Kafka wire error namespace compatibility
  A released Kafka error must retain its exact wire number. Broker-private
  errors must stay beyond both the released and upcoming Kafka namespaces.

  Scenario: Released errors keep Kafka's wire assignments
    Given the published Kafka release and trunk error namespaces
    When the broker's released and private wire error codes are enumerated
    Then every released error keeps Kafka's name and numeric wire code

  Scenario: Private errors cannot collide with Kafka's current or upcoming assignments
    Given the published Kafka release and trunk error namespaces
    When the broker's released and private wire error codes are enumerated
    Then every private error is at least 1000 and above both Kafka namespaces
