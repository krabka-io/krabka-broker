Feature: tests client telemetry

  Scenario: api versions advertises telemetry apis
    Then client telemetry: api versions advertises telemetry apis

  Scenario: api versions withholds telemetry apis without a receiver
    Then client telemetry: api versions withholds telemetry apis without a receiver

  Scenario: get telemetry subscriptions with nil id returns assigned id and no subscription
    Then client telemetry: get telemetry subscriptions with nil id returns assigned id and no subscription

  Scenario: get telemetry subscriptions with set id echoes it
    Then client telemetry: get telemetry subscriptions with set id echoes it

  Scenario: push telemetry unknown instance rejected
    Then client telemetry: push telemetry unknown instance rejected

  Scenario: push telemetry happy path after subscription
    Then client telemetry: push telemetry happy path after subscription

  Scenario: push telemetry stale subscription id rejected
    Then client telemetry: push telemetry stale subscription id rejected

  Scenario: push telemetry unsupported compression rejected
    Then client telemetry: push telemetry unsupported compression rejected

  Scenario: closing the connection drops the client instance
    Then client telemetry: closing the connection drops the client instance
