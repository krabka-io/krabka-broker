Feature: Kafka offset lookup timestamps and protocol compatibility
  Special log positions have fixed wire timestamps. Newer lookup modes must
  be refused by request versions that cannot express them.

  Scenario Outline: Named positions retain their Kafka wire timestamps
    Given the client requests the <position> log position
    When the position is encoded as a Kafka request timestamp
    Then the wire timestamp is <timestamp>

    Examples:
      | position                | timestamp |
      | earliest                | -2        |
      | latest                  | -1        |
      | maximum-timestamp       | -3        |
      | earliest-local          | -4        |
      | latest-tiered           | -5        |
      | earliest-pending-upload | -6        |

  Scenario Outline: Offset lookup modes require their introducing protocol version
    Given a ListOffsets version <version> request carries timestamp <timestamp>
    When the broker classifies the ListOffsets timestamp
    Then the timestamp is <support> by that protocol version

    Examples:
      | timestamp | version | support     | boundary                     |
      | -2        | 0       | supported   | earliest                     |
      | -1        | 0       | supported   | latest                       |
      | -3        | 6       | unsupported | before maximum timestamp     |
      | -3        | 7       | supported   | maximum timestamp introduced |
      | -4        | 7       | unsupported | before local lookup          |
      | -4        | 8       | supported   | local lookup introduced      |
      | -5        | 8       | unsupported | before tiered lookup         |
      | -5        | 9       | supported   | tiered lookup introduced     |
      | -6        | 10      | unsupported | before pending upload lookup |
      | -6        | 11      | supported   | pending upload introduced    |
      | -7        | 11      | unsupported | unknown negative sentinel    |
      | 0         | 1       | supported   | ordinary timestamp lookup    |
