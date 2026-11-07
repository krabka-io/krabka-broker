Feature: Each event is written as one line of JSON

  The Cloud Logging agent on GKE parses single-line JSON written to stdout.
  A line that is not one whole JSON object is ingested as plain text, and its
  fields are lost.

  Rule: Every entry is one line of JSON, whatever its text holds

    Scenario Outline: A message with awkward text stays on one line
      When the broker logs a message containing <text>
      Then the output is one JSON line
      And the entry message reads back unchanged

      Examples:
        | case                                  | text                   |
        | a line break would split the line     | a line break           |
        | a double quote would end the string   | a double quote         |
        | a backslash would start an escape     | a backslash            |
        | a terminal escape would colour a TTY  | a terminal escape code |

  Rule: Entries carry no terminal colour codes

    Scenario: An error is written without colour
      When the broker logs an event at level ERROR
      Then the output holds no terminal escape code

  Rule: An event below the configured log level is not written

    Scenario: A debug event is dropped at log level INFO
      Given the log level has been set to INFO
      When the broker logs an event at level DEBUG
      Then nothing is written

    Scenario: An event at the configured log level is written
      Given the log level has been set to INFO
      When the broker logs an event at level INFO
      Then one entry is written
