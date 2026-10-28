Feature: ListOffsets applies Kafka lagging-high-watermark fencing to lookup results
  Scenario Outline: Lookup kind and candidate position determine whether an offset is available
    Given a lagging high watermark bounds <kind> offset candidate <candidate> before offset 3
    When ListOffsets decides whether the lagging high watermark fences the result
    Then the candidate is fenced <expected>
    Examples:
      | kind                    | candidate | expected |
      | latest                  | 7         | true     |
      | latest                  | 0         | true     |
      | earliest                | 0         | false    |
      | earliest-local          | 0         | false    |
      | timestamp               | 2         | false    |
      | timestamp               | 3         | true     |
      | timestamp               | 5         | true     |
      | timestamp               | -1        | true     |
      | maximum-timestamp       | 2         | false    |
      | maximum-timestamp       | 3         | true     |
      | maximum-timestamp       | -1        | true     |
      | latest-tiered           | 2         | false    |
      | latest-tiered           | 3         | true     |
      | latest-tiered           | -1        | false    |
      | earliest-pending-upload | 2         | false    |
      | earliest-pending-upload | 4         | true     |
      | earliest-pending-upload | -1        | false    |
