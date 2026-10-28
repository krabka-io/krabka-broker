Feature: New local partitions use both configured log directories
  Scenario: New partitions spread across both directories and remain discoverable
    Given one broker uses two independent log directories
    When topic t is created with 6 materialized local partitions
    Then both log directories hold partitions totaling 6
    When the client describes all configured log directories
    Then two successful directory reports contain nonfuture nonnegative partition sizes for exactly
      | partition |
      | 0 |
      | 1 |
      | 2 |
      | 3 |
      | 4 |
      | 5 |
