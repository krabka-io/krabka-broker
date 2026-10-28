Feature: Fetch read failures and directory health

  Scenario Outline: Read exceptions produce partition errors and only disk failures fence directories
    Given an online partition log whose read fails with <failure>
    When Fetch settles the failed read for response partition 4
    Then the full Fetch refusal has code <code> and the directory offline state is <offline>

    Examples:
      | failure | code | offline |
      | IO-EIO | 56 | true |
      | requested-0-before-log-start-5 | 1 | false |
      | requested-0-before-local-start-5 | 1 | false |
      | corrupt-record-batch-header | 2 | false |
      | task-panic-boom | -1 | false |
