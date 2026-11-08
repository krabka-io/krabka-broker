Feature: Benchmark producer request ordering
  Scenario Outline: Idempotence limits concurrent produce requests
    Given producer idempotence is <enabled>
    When the benchmark producer's in-flight limit is configured
    Then at most <maximum> produce requests are in flight

    Examples:
      | case                                       | enabled | maximum |
      | an idempotent producer keeps batch order   | true    | 5       |
      | a non-idempotent producer pipelines freely | false   | 128     |
