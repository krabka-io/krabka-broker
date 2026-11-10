Feature: ListTransactions filters follow Kafka

  Rule: The duration filter keeps a transaction that has run strictly longer than it

    Scenario Outline: A transaction that has run longer than the filter is listed
      Given a transaction started at <start> ms
      When an admin lists the transactions running longer than <filter> ms at 10000 ms
      Then the transaction is listed

      Examples:
        | case                                       | start | filter |
        | it ran one millisecond longer              | 8999  | 1000   |
        | a negative filter keeps a future start     | 20000 | -2     |

    Scenario Outline: A transaction that has not run longer than the filter is not listed
      Given a transaction started at <start> ms
      When an admin lists the transactions running longer than <filter> ms at 10000 ms
      Then the transaction is not listed

      Examples:
        | case                                       | start | filter |
        | it ran exactly as long as the filter       | 9000  | 1000   |
        | a zero filter drops a start in the future  | 11000 | 0      |

  Rule: A transactional id pattern matches the whole transactional id

    Scenario: Without a pattern every transactional id is listed
      Given a transaction with transactional id anything
      When an admin lists the transactions without a transactional id pattern
      Then the transaction is listed

    Scenario: An empty pattern lists every transactional id
      Given a transaction with transactional id anything
      When an admin lists the transactions with an empty transactional id pattern
      Then the transaction is listed

    Scenario Outline: A pattern that matches the whole id lists the transaction
      Given a transaction with transactional id <id>
      When an admin lists the transactions with the transactional id pattern <pattern>
      Then the transaction is listed

      Examples:
        | case                                   | id        | pattern       |
        | a wildcard suffix                      | txn-alpha | txn-.*        |
        | the longer alternative of a prefix     | txn-long  | txn\|txn-long |

    Scenario: A pattern that matches only part of the id does not list the transaction
      Given a transaction with transactional id prefix-txn-alpha
      When an admin lists the transactions with the transactional id pattern txn-.*
      Then the transaction is not listed

  Rule: A pattern is compiled as RE2J, which refuses what it cannot run in linear time

    Scenario Outline: A pattern RE2J does not support is refused
      When an admin attempts to list the transactions with the transactional id pattern <pattern>
      Then the pattern is refused as an invalid regular expression

      Examples:
        | case                    | pattern    |
        | a lookahead             | (?=txn-).* |
        | a backreference         | (txn)-\\1  |
        | a possessive quantifier | a*+        |
        | an unclosed group       | (unclosed  |
