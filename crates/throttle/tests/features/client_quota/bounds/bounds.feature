Feature: A client quota's bound is applied as configured

  An operator sets a client quota with kafka-configs, and the broker applies
  the new bound to the running quota. Kafka's updateQuotaMetricConfigs swaps
  the bound and keeps the recorded samples. request_percentage is a double, so
  its bound can be a fraction: a request quota of 0.05 percent allows 0.5 ms
  of request handler time each second.

  Depends on: the sample window - ../measurement/measurement.feature

  Rule: A quota change judges the traffic already measured against the new bound

    Scenario Outline: A changed quota throttles the measured traffic at the new bound
      Given a client quota of <before> bytes per second
      And the client has sent 15000 bytes
      And the quota has been changed to <after> bytes per second
      When the client sends an empty request
      Then the client is throttled for <throttle> ms

      Examples:
        | case    | before | after | throttle |
        | raised  | 1000   | 1200  | 2500     |
        | lowered | 2000   | 1000  | 5000     |

  Rule: A fractional bound is held as configured

    Scenario: Handler time at a fractional request quota is within the quota
      Given a request quota of 0.05 percent
      When the client's requests take 5 ms of handler time
      Then the client is not throttled

    Scenario: Handler time past a fractional request quota is throttled
      Given a request quota of 0.05 percent
      When the client's requests take 6 ms of handler time
      Then the client is throttled for 2000 ms

  Rule: A bound that is not a positive rate throttles nothing

    Scenario Outline: A quota without a positive rate never throttles
      Given a client quota of <bound> bytes per second
      When the client sends 1000000000 bytes
      Then the client is not throttled

      Examples:
        | case               | bound |
        | a zero rate        | 0     |
        | a negative rate    | -1    |
        | an unlimited rate  | inf   |
        | a rate that is NaN | NaN   |
