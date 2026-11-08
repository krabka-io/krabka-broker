Feature: A client quota charges every request in full and keeps the overage as debt

  KIP-13 and KIP-124. A broker meters each client quota entity with a token
  bucket: the bucket refills at the quota rate (consumer_byte_rate,
  producer_byte_rate or request_percentage) and holds one quota window of it,
  quota.window.num times quota.window.size.seconds, 11 seconds by default.

  Like Kafka's ClientQuotaManager.recordAndGetThrottleTimeMs, the broker
  records the whole request whether or not the quota covers it. What the
  balance cannot cover is debt, and the broker throttles the client for the
  debt divided by the rate, as Kafka's QuotaUtils.throttleTime does. The
  scenarios below use a quota of 1000 bytes per second with an 11 second
  window unless they say otherwise.

  Rule: A charge the balance covers leaves no debt

    Scenario Outline: A request within one quota window of traffic is not throttled
      Given a client quota of 1000 bytes per second
      When the client sends <sent> bytes
      Then the client owes 0 bytes

      Examples:
        | case                       | sent  |
        | part of the window         | 4000  |
        | exactly the whole window   | 11000 |

  Rule: A charge past the balance is taken in full and the rest is owed

    Scenario: One byte past the window is owed
      Given a client quota of 1000 bytes per second
      When the client sends 11001 bytes
      Then the client owes 1 bytes

    Scenario: The next request pays for the overage of the one before
      Given a client quota of 1000 bytes per second
      And the client has sent 15000 bytes
      When the client sends 1000 bytes
      Then the client owes 5000 bytes

  Rule: A bounded quota keeps no more debt than the bound repays

    Scenario: A request quota owes at most one throttle bound of debt
      Given a client quota of 1000 bytes per second
      When the client sends 20000 bytes under a throttle bound of 1 seconds
      Then the client owes 1000 bytes

  Rule: A throttled fetch gives its charge back

    Scenario: A refund repays the debt before it refills the balance
      Given a client quota of 1000 bytes per second
      And the client has sent 15000 bytes
      When the broker refunds 15000 bytes to the client
      Then the client can send 11000 bytes before it owes anything

  Rule: A quota without a rate never charges

    Scenario: An unlimited client owes nothing
      When the client sends 1000000000 bytes
      Then the client owes 0 bytes
