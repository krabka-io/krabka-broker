Feature: A request refused before any handler reads it carries Kafka's error message

  Rule: The refusal keeps the exception's own message unless the code is UNKNOWN_SERVER_ERROR or the message is the code's default

    Scenario: A refused DescribeAcls request keeps the exception's own message
      Given a request is refused with ILLEGAL_SASL_STATE carrying its own message
      When the broker answers a DescribeAcls request with that refusal
      Then the DescribeAcls response carries ILLEGAL_SASL_STATE with the exception's own message

    Scenario Outline: A refused DescribeAcls request drops a message that Kafka hides
      Given a request is refused with <code> carrying <message>
      When the broker answers a DescribeAcls request with that refusal
      Then the DescribeAcls response carries <code> with no message

      Examples:
        | case                                      | code                 | message                    |
        | the code's default message is dropped     | ILLEGAL_SASL_STATE   | the code's default message |
        | an unknown server error hides its message | UNKNOWN_SERVER_ERROR | its own message            |
