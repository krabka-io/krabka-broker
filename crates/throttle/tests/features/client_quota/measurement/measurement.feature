Feature: A client quota measures a client's rate over its sample window

  KIP-13 and KIP-124. A broker meters each client quota entity's
  producer_byte_rate, consumer_byte_rate and request_percentage with Kafka's
  ClientQuotaManager quota sensor: a Rate over quota.window.num samples of
  quota.window.size.seconds each, 11 samples of 1 second by default. The rate
  is what the samples hold over the time since the oldest kept sample
  started, and while that time is shorter than quota.window.num - 1 samples
  it is padded up to them. The scenarios below use a client quota of 1000
  bytes per second with the default window unless they say otherwise.

  Rule: A new client is measured over quota.window.num - 1 samples

    Scenario Outline: A first burst that fills the padded window is not throttled
      Given the broker sets quota.window.num to <samples>
      And the broker sets quota.window.size.seconds to <seconds>
      And a client quota of 1000 bytes per second
      When the client sends <sent> bytes
      Then the client is not throttled

      Examples:
        | case                                | samples | seconds | sent  |
        | the default window pads ten seconds | 11      | 1       | 10000 |
        | longer samples pad longer           | 11      | 2       | 20000 |
        | two samples pad one                 | 2       | 1       | 1000  |

    Scenario Outline: One byte past the padded window is throttled
      Given the broker sets quota.window.num to <samples>
      And the broker sets quota.window.size.seconds to <seconds>
      And a client quota of 1000 bytes per second
      When the client sends <sent> bytes
      Then the client is throttled for 1 ms

      Examples:
        | case                                | samples | seconds | sent  |
        | the default window pads ten seconds | 11      | 1       | 10001 |
        | longer samples pad longer           | 11      | 2       | 20001 |
        | two samples pad one                 | 2       | 1       | 1001  |

  Rule: A window of one sample is not padded and is never shorter than 1 ms

    Scenario: One byte at once is within the bound of a single-sample window
      Given the broker sets quota.window.num to 1
      And a client quota of 1000 bytes per second
      When the client sends 1 bytes
      Then the client is not throttled

    Scenario: A first burst in a single-sample window is measured over 1 ms
      Given the broker sets quota.window.num to 1
      And a client quota of 1000 bytes per second
      When the client sends 15000 bytes
      Then the client is throttled for 14999 ms

  Rule: Past the padding the rate is measured over the time since the oldest kept sample started

    Scenario: A client at its bound for nine seconds is still within the padding
      Given a client quota of 1000 bytes per second
      And the client has sent 1000 bytes once a second for 9 seconds
      When the client sends 1000 bytes
      Then the client is not throttled

    Scenario Outline: A client past the padding is throttled for the wait back to its bound
      Given a client quota of 1000 bytes per second
      And the client has sent 1000 bytes once a second for <seconds> seconds
      When the client sends <sent> bytes
      Then the client is throttled for <throttle> ms

      Examples:
        | case                                   | seconds | sent | throttle |
        | the bound once more just past padding  | 10      | 1000 | 1000     |
        | the bound once more long past padding  | 30      | 1000 | 1000     |
        | five seconds of the bound at once      | 10      | 5000 | 5000     |
