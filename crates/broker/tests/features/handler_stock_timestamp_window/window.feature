Feature: Unconfigured topics retain Kafka's one-hour future timestamp window
  Scenario Outline: Both append representations enforce the same default clock bound
    Given an unconfigured topic receives timestamps <timestamps>
    When the batch is prepared through both verbatim and owned compression paths
    Then both preparation paths return timestamp error <code>
    Examples:
      | timestamps                                  | code |
      | 1000 and 2000 milliseconds since the epoch   | 0    |
      | now and exactly one hour ahead               | 0    |
      | now and one hour plus one minute ahead       | 32   |
