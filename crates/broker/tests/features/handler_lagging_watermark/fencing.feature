Feature: ListOffsets applies Kafka lagging-high-watermark fencing to lookup results

  Rule: A consumer's lookup is fenced only while the high watermark lags its epoch start

    Scenario Outline: The fence follows the requester and the high watermark
      Given the leader epoch started at offset 4
      And the high watermark is at offset <high watermark>
      When <requester> looks up an offset with ListOffsets v5
      Then the lookup is <outcome>

      Examples:
        | case                          | requester           | high watermark | outcome              |
        | caught up exactly             | a consumer          | 4              | not fenced           |
        | caught up past the epoch      | a consumer          | 6              | not fenced           |
        | lagging by one                | a consumer          | 3              | OFFSET_NOT_AVAILABLE |
        | a follower is never fenced    | a follower          | 3              | not fenced           |
        | a debugging replica neither   | a debugging replica | 3              | not fenced           |

    Scenario: A leader with no epoch start is never fenced
      Given the leader has no epoch start offset
      And the high watermark is at offset 3
      When a consumer looks up an offset with ListOffsets v5
      Then the lookup is not fenced

  Rule: A version older than v5 learns of the fence as LEADER_NOT_AVAILABLE

    Scenario Outline: The fence error depends on the ListOffsets version
      Given the leader epoch started at offset 4
      And the high watermark is at offset 3
      When a consumer looks up an offset with ListOffsets v<version>
      Then the lookup is <outcome>

      Examples:
        | case                           | version | outcome              |
        | the first version with a code  | 5       | OFFSET_NOT_AVAILABLE |
        | the last version without one   | 4       | LEADER_NOT_AVAILABLE |
        | the first version              | 1       | LEADER_NOT_AVAILABLE |

  Rule: A lagging high watermark fences a result by its lookup kind and offset

    Scenario Outline: Lookup kind and candidate position determine whether an offset is available
      Given a lagging high watermark bounds <kind> offset candidate <candidate> before offset 3
      When ListOffsets decides whether the lagging high watermark fences the result
      Then the candidate is fenced <expected>

      Examples:
        | case                                    | kind                    | candidate | expected |
        | latest is fenced past the bound         | latest                  | 7         | true     |
        | latest is fenced at any offset          | latest                  | 0         | true     |
        | earliest is never fenced                | earliest                | 0         | false    |
        | earliest local is never fenced          | earliest-local          | 0         | false    |
        | a timestamp below the bound             | timestamp               | 2         | false    |
        | a timestamp at the bound                | timestamp               | 3         | true     |
        | a timestamp past the bound              | timestamp               | 5         | true     |
        | a timestamp that found nothing          | timestamp               | -1        | true     |
        | a maximum timestamp below the bound     | maximum-timestamp       | 2         | false    |
        | a maximum timestamp at the bound        | maximum-timestamp       | 3         | true     |
        | a maximum timestamp that found nothing  | maximum-timestamp       | -1        | true     |
        | a latest tiered below the bound         | latest-tiered           | 2         | false    |
        | a latest tiered at the bound            | latest-tiered           | 3         | true     |
        | an empty tier answers minus one         | latest-tiered           | -1        | false    |
        | a pending upload below the bound        | earliest-pending-upload | 2         | false    |
        | a pending upload past the bound         | earliest-pending-upload | 4         | true     |
        | nothing pending answers minus one       | earliest-pending-upload | -1        | false    |
