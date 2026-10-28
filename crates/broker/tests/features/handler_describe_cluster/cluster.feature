Feature: DescribeCluster validates endpoint type and represents Kafka cluster identity
  Scenario Outline: Endpoint type errors precede authorization and retain Kafka response defaults
    Given a broker listener denies all authorization
    And a DescribeCluster version <version> request asks for endpoint type <endpoint>
    When the broker listener handles DescribeCluster
    Then the endpoint refusal is code <code> message <message> with the default broker endpoint and no cluster data
    Examples:
      | version | endpoint | code | message                                                                                                               |
      | 2       | 2        | 114  | The request was sent to an endpoint of type BROKER, but we wanted an endpoint of type CONTROLLER                       |
      | 2       | 0        | 115  | Unsupported endpoint type 0                                                                                           |
      | 1       | 3        | 115  | Unsupported endpoint type 3                                                                                           |

  Scenario: The cluster identifier uses Kafka unpadded base64
    Given the broker cluster UUID has bytes 1 through 16
    When the broker listener handles DescribeCluster
    Then the reported cluster ID is AQIDBAUGBwgJCgsMDQ4PEA
