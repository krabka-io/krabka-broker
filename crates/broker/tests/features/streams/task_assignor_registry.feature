Feature: Native Streams task assignor selection
  Scenario Outline: Selecting a registered implementation executes its own assignment logic
    Given native Streams assignors first and second pin tasks to different members
    When the Streams group selects <selection> and computes its target
    Then the selected assignor is <reported> and all active tasks belong to <owner>

    Examples:
      | selection | reported | owner |
      | default   | first    | a     |
      | first     | first    | a     |
      | second    | second   | b     |
