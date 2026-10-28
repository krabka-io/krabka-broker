Feature: Benchmark producer request ordering
  Scenario Outline: Idempotence limits concurrent produce requests
    Given producer idempotence is <enabled>
    When the benchmark producer's in-flight limit is configured
    Then at most <maximum> produce requests are in flight

    Examples:
      | enabled | maximum |
      | true    | 5       |
      | false   | 128     |
