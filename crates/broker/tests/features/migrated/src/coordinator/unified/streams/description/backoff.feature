Feature: Streams description retry backoff

  Rule: A retry varies by Kafka's twenty percent jitter

    Scenario: Retry backoff varies by Kafka twenty percent jitter
      Given a Streams description retry requests 1000 independent jitter samples
      When the retry scheduler draws its randomized backoff multipliers
      Then all sampled multipliers lie between 800 inclusive and 1200 exclusive per mille

  Rule: Each retry window doubles from 30 seconds and never passes an hour

    Scenario Outline: The retry window after earlier attempts
      When the coordinator sizes the retry window after <attempts> attempts at jitter <jitter> per mille
      Then the retry window lasts <millis> milliseconds

      Examples:
        | case                                  | attempts   | jitter | millis  |
        | the first window                      | 0          | 1000   | 30000   |
        | the second window doubles             | 1          | 1000   | 60000   |
        | the last window below the cap         | 6          | 1000   | 1920000 |
        | the first window at the cap           | 7          | 1000   | 3600000 |
        | far past the cap                      | 40         | 1000   | 3600000 |
        | the most attempts                     | 4294967295 | 1000   | 3600000 |
        | the lowest jitter shortens the window | 0          | 800    | 24000   |
        | the highest jitter lengthens it       | 0          | 1199   | 35970   |
        | high jitter just below the cap        | 6          | 1199   | 2302080 |
        | low jitter shortens the capped window | 7          | 800    | 2880000 |
        | high jitter cannot pass the cap       | 7          | 1199   | 3600000 |
