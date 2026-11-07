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
        | an offset past the signed range stays exact | offset   | number  | 18446744073709551615 |
        | a ratio keeps its fraction                  | ratio    | number  | 0.5                  |

  Rule: A value with no JSON type is written as its text

    Scenario Outline: A value without a JSON type becomes text
      When the broker logs an event with a <field> field
      Then the entry field <field> is the text <value>

      Examples:
        | case                     | field    | value         |
        | a debug-formatted list   | replicas | [1, 2, 3]     |
        | a displayed peer address | peer     | 10.0.0.7:9092 |
        | an error                 | error    | disk full     |

  Rule: Only the event's own fields are written, not those of the span it runs in

    Scenario: A request span's topic stays off the entry
      Given the broker is handling a produce request for topic orders
      When the broker logs that a batch was appended
      Then the entry has no topic field
