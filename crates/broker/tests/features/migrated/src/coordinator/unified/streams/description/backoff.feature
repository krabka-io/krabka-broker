Feature: Streams description retry jitter
  Scenario: Retry backoff varies by Kafka twenty percent jitter
    Given a Streams description retry requests 1000 independent jitter samples
    When the retry scheduler draws its randomized backoff multipliers
    Then all sampled multipliers lie between 800 inclusive and 1200 exclusive per mille
