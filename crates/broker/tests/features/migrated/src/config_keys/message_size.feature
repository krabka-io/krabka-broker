Feature: Kafka maximum record-batch configuration domain
  A topic accepts a whole nonnegative 32-bit batch limit, including zero,
  and rejects negative, oversized, missing, and nonnumeric values.

  Scenario Outline: The maximum batch size is a nonnegative Kafka INT
    Given the quoted max.message.bytes override is <value>
    When the topic max.message.bytes override is validated
    Then the max.message.bytes override is accepted <accepted>

    Examples:
      | value        | accepted |
      | "0"          | true     |
      | "1048588"    | true     |
      | "2147483647" | true     |
      | "-1"         | false    |
      | "2147483648" | false    |
      | "abc"        | false    |
      | ""           | false    |
