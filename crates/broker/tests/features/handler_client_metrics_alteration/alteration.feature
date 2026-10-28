Feature: IncrementalAlterConfigs validates the merged client metrics subscription
  Scenario Outline: Invalid alterations return precise Kafka errors without writing records
    Given subscription sub-a alters key <key> with operation <operation> and value <value>
    When the alteration merges with interval 60000 and metrics a.
    Then the client metrics alteration returns code <code> message <message> and stages no record
    Examples:
      | key         | operation | value                      | code | message                                                                                              |
      | bogus       | 0         | x                          | 42   | Unknown client metrics configuration: bogus                                                           |
      | interval.ms | 0         | 99                         | 42   | Invalid value 99 for interval.ms, interval must be between 100 and 3600000 (1 hour)                     |
      | interval.ms | 0         | abc                        | 40   | Invalid value abc for configuration interval.ms: Not a number of type INT                             |
      | metrics     | 0         | a.,,b.                     | 40   | Configuration 'metrics' values must not be empty.                                                      |
      | metrics     | 0         | a., a.                     | 40   | Configuration 'metrics' values must not be duplicated.                                                 |
      | match       | 0         | client_id=a,,client_id=b    | 40   | Configuration 'match' values must not be empty.                                                        |
      | match       | 0         | client_id=a=b              | 40   | Illegal client matching pattern: client_id=a=b                                                        |
      | match       | 0         | client_id=                 | 40   | Illegal client matching pattern: client_id=                                                           |
      | match       | 0         | client_foo=x               | 40   | Illegal client matching pattern: client_foo=x                                                         |
      | match       | 0         | client_id=[x               | 40   | Illegal client matching pattern: client_id=[x                                                         |
      | match       | 0         | client_id=(?P<n>x)          | 40   | Illegal client matching pattern: client_id=(?P<n>x)                                                    |
      | metrics     | 2         | b.                         | 40   | Can't APPEND to key metrics because its type is not LIST.                                              |
      | metrics     | 3         | a.                         | 40   | Can't SUBTRACT to key metrics because its type is not LIST.                                            |
