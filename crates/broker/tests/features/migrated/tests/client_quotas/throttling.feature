Feature: Byte and request quotas report and account for actual applied throttles
  Background:
    Given a SASL quota broker has super user admin and ordinary user alice

  Scenario: producer byte rate throttles produce
    Given topic throttle-produce permits alice to Write
    And alice has producer_byte_rate quota 128
    When alice produces 8 records of 1024 bytes to the topic
    Then the produce partition succeeds and reports a positive throttle delay

  Scenario: request percentage throttles produce
    Given topic throttle-request permits alice to Write
    And alice has request_percentage quota 0.001
    When alice produces 1 records of 16 bytes to the topic
    Then the produce partition succeeds and reports a positive throttle delay

  Scenario: producer byte rate throttle moves the throttle metrics
    Given topic throttle-metrics permits alice to Write
    And alice has producer_byte_rate quota 128
    When alice produces 8 records of 1024 bytes to the topic
    Then the produce partition succeeds and reports a positive throttle delay
    And Produce latency and producer quota metrics cover the delay while request quota credit stays zero

  Scenario: consumer byte rate throttles fetch
    Given topic throttle-fetch permits alice to Read
    And alice has consumer_byte_rate quota 128
    And admin has stored eight 1024 byte records in throttle-fetch
    When alice fetches throttle-fetch after the Read grant applies
    Then the throttled fetch reports a positive delay with no records rows errors or session

  Scenario: user client tuple overrides user specific
    Given topic precedence-topic permits alice to Write
    And alice producer rate is 8192 but the krabka-quota-test user-client tuple rate is 128
    When alice produces 8 records of 1024 bytes to the topic
    Then the produce partition succeeds and reports a positive throttle delay

  Scenario: request percentage throttle is echoed on a patched api
    Given alice uses one authenticated connection for the requests
    When the connection sends an unthrottled AddOffsetsToTxn request
    Then AddOffsetsToTxn initially reports zero throttle
    Given alice has request_percentage quota 0.001
    When the same connection drives AddOffsetsToTxn until the request quota throttles
    Then the positive AddOffsetsToTxn throttle leaves every other response field unchanged

  Scenario: request percentage throttle is reported on api versions
    Given alice uses one authenticated connection for the requests
    When the connection sends an unthrottled API versions v3 request
    Then API versions initially succeeds with zero throttle
    Given alice has request_percentage quota 0.001
    When the same connection drives API versions v3 until the request quota throttles
    Then API versions reports a positive capped delay and advancing epoch with all advertised APIs and features unchanged
