Feature: Kafka DescribeConfigs primitive type encoding
  Kafka clients and generated reference pages receive the same six primitive
  labels and protocol type bytes measured on Apache Kafka 4.3.1.

  Scenario: Primitive type labels preserve Kafka wire assignments and order
    Given the six Kafka primitive config types Boolean, String, Int, Long, Double, and List
    When the primitive config labels and DescribeConfigs wire types are encoded
    Then the ordered primitive config labels and wire bytes are:
      | label   | wire |
      | boolean | 1    |
      | string  | 2    |
      | int     | 3    |
      | long    | 5    |
      | double  | 6    |
      | list    | 7    |
