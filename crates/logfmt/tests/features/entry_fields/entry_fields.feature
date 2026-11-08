Feature: Event fields become queryable top-level entry fields

  Cloud Logging shows the message field as the summary line of an entry and
  keeps every other field in jsonPayload, where an operator filters on it, for
  example jsonPayload.listener=PLAIN. The formatter therefore writes the
  message and the event fields side by side at the top level, not nested.

  Rule: The event message is the entry summary

    Scenario: A connection event is summarised by its message
      When the broker logs that a connection opened
      Then the entry message is connection opened

    Scenario: A formatted message reads as plain text
      When the broker logs that 3 connections opened
      Then the entry message is opened 3 connections

    Scenario: An event with only fields has no summary
      When the broker logs an event with only an offset field
      Then the entry has no message

  Rule: Event fields sit at the top level beside the message and the entry keys

    Scenario: A connection event becomes one flat entry
      When the broker logs that a connection opened
      Then the entry apart from its timestamp reads
        """
        {
          "severity": "INFO",
          "target": "krabka_broker::network::dispatch",
          "message": "connection opened",
          "listener": "PLAIN",
          "sasl": false,
          "port": 9092
        }
        """

  Rule: A field keeps its JSON type, so a filter compares numbers as numbers

    Scenario Outline: A typed field keeps its type
      When the broker logs an event with a <field> field
      Then the entry field <field> is the <type> <value>

      Examples:
        | case                                        | field    | type    | value                |
        | text stays text                             | listener | text    | PLAIN                |
        | a flag stays a boolean                      | sasl     | boolean | false                |
        | a negative delta stays signed               | delta    | number  | -3                   |
        | a ratio keeps its fraction                  | ratio    | number  | 0.5                  |
        | a NaN has no JSON number, so it is null     | load     | null    | null                 |
        | an infinity has no JSON number either       | rate     | null    | null                 |
        | a negative infinity has none either         | headroom | null    | null                 |

  Rule: A value with no JSON type is written as its text

    Scenario Outline: A value without a JSON type becomes text
      When the broker logs an event with a <field> field
      Then the entry field <field> is the text <value>

      Examples:
        | case                     | field    | value         |
        | a debug-formatted list   | replicas | [1, 2, 3]     |
        | a displayed peer address | peer     | 10.0.0.7:9092 |
        | an error                 | error    | disk full     |

  Rule: An integer beyond 2^53 is written as text, so Cloud Logging keeps every digit

    Cloud Logging stores a JSON number as a double, which holds every integer up
    to 2^53 exactly and rounds the ones past it.

    Scenario Outline: An integer stays a number up to 2^53 and becomes text past it
      When the broker logs a <width> integer field of <value>
      Then the entry integer field is the <type> <value>

      Examples:
        | case                                        | width            | value                                   | type   |
        | 2^53 is still exact as a number             | unsigned 64-bit  | 9007199254740992                        | number |
        | one past 2^53 would round, so it is text    | unsigned 64-bit  | 9007199254740993                        | text   |
        | an offset past the signed range stays exact | unsigned 64-bit  | 18446744073709551615                    | text   |
        | a signed value past 2^53 is text            | signed 64-bit    | 9007199254740993                        | text   |
        | minus 2^53 is still exact as a number       | signed 128-bit   | -9007199254740992                       | number |
        | one below -2^53 would round, so it is text  | signed 128-bit   | -9007199254740993                       | text   |
        | a small wide value stays a number           | unsigned 128-bit | 42                                      | number |
        | the widest value is text                    | unsigned 128-bit | 340282366920938463463374607431768211455 | text   |

  Rule: An entry carries the fields of the span its event runs in

    Scenario: A request span's topic is written on the entry
      Given the broker is handling a produce request for topic orders
      When the broker logs that a batch was appended
      Then the entry field topic reads orders

    Scenario: A field the span records after it opened is written on the entry
      Given the broker is handling a produce request that has recorded error code MESSAGE_TOO_LARGE
      When the broker logs that a batch was appended
      Then the entry field error_code reads MESSAGE_TOO_LARGE

    Scenario: The event's own field wins over the span's field of the same name
      Given the broker is handling a produce request for topic orders
      When the broker logs that a batch was appended to topic payments
      Then the entry field topic reads payments

    Scenario: Only the current span's fields are written, not those of the spans around it
      Given the broker is handling a produce request for topic orders
      When the broker logs that a segment rolled for partition 3 within the request
      Then the entry has no topic field
