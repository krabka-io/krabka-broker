Feature: A client over its quota is throttled for the wait back to the bound

  Kafka's QuotaUtils.throttleTime. A client whose measured rate is above the
  quota bound is throttled for (rate - bound) / bound times the window the
  rate was measured over, rounded to the nearest millisecond. It is the wait
  that brings the rate back to the bound if the client sends nothing more. A
  rate exactly at the bound is within the quota.

  Depends on: the sample window - ../measurement/measurement.feature

  Rule: The throttle grows with the overage

    Scenario Outline: A first burst over the quota is throttled in proportion to its overage
      Given a client quota of 1000 bytes per second
      When the client sends <sent> bytes
      Then the client is throttled for <throttle> ms

      Examples:
        | case                         | sent  | throttle |
        | half again the padded window | 15000 | 5000     |
        | twice the padded window      | 20000 | 10000    |

  Rule: The throttle is rounded to the nearest millisecond

    Scenario: An overage worth a third of a millisecond is not throttled
      Given a client quota of 3000 bytes per second
      When the client sends 30001 bytes
      Then the client is not throttled

    Scenario: An overage worth two thirds of a millisecond is throttled for 1 ms
      Given a client quota of 3000 bytes per second
      When the client sends 30002 bytes
      Then the client is throttled for 1 ms
