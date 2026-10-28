Feature: Kafka cluster identity interchange
  Metadata publishes Kafka's UUID representation, while incoming requests can
  name that same cluster using either Kafka's or Java's UUID spelling.

  Scenario: Metadata uses Kafka's URL-safe unpadded cluster UUID
    Given the cluster UUID is 01020304-0506-0708-090a-0b0c0d0e0f10
    When the broker encodes the cluster UUID for Kafka metadata
    Then the metadata cluster ID is AQIDBAUGBwgJCgsMDQ4PEA

  Scenario Outline: Requests accept both supported representations of the same cluster
    Given the cluster UUID is 00000000-0000-0000-0000-0000000000ff
    And a request identifies the cluster as <requested>
    When the broker checks the request's cluster identity
    Then the request identity is accepted <accepted>

    Examples:
      | requested                            | accepted |
      | AAAAAAAAAAAAAAAAAAAA_w                | true     |
      | 00000000-0000-0000-0000-0000000000ff   | true     |
      | different                            | false    |
