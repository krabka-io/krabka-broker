Feature: A client quota records every request in full

  Kafka's ClientQuotaManager.recordAndGetThrottleTimeMs records a request
  before it checks the quota, so a request over the quota still counts, and
  the requests after it pay for it while its sample is in the window. A
  sample is forgotten once its last record is quota.window.num samples old,
  11 seconds by default. A throttled fetch sends no records, so the broker
  takes its bytes back off the quota, as unrecordQuotaSensor does.

  Depends on: the sample window - ../measurement/measurement.feature

  Rule: A burst is paid for while its sample is in the window

    Scenario Outline: Later requests pay for an earlier burst
      Given a client quota of 1000 bytes per second
      And the client has sent 15000 bytes
      And <later> ms have passed
      When the client sends an empty request
      Then the client is throttled for <throttle> ms

      Examples:
        | case                                    | later | throttle |
        | the window is still padded              | 5000  | 5000     |
        | the padding has just run out            | 10000 | 5000     |
        | the last millisecond the sample is kept | 10999 | 4001     |

    Scenario: A burst is forgotten once its sample is quota.window.num samples old
      Given a client quota of 1000 bytes per second
      And the client has sent 15000 bytes
      And 11000 ms have passed
      When the client sends an empty request
      Then the client is not throttled

  Rule: A throttled fetch gives its bytes back

    Scenario: The bytes of a throttled fetch no longer count against the client
      Given a client quota of 1000 bytes per second
      And the client has fetched 15000 bytes
      When the broker takes back the 15000 bytes of the throttled fetch
      Then the client can fetch 10000 bytes before it is throttled
