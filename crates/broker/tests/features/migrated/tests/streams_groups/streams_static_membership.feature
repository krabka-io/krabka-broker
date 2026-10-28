Feature: Unsupported static streams membership leaves coordinator state untouched

  Scenario: every static member request is refused and changes nothing
    Given a streams-enabled broker holds topic static-input with 2 partitions for group static-app
    When a stateless streams member joins and converges on two active tasks
    Then the streams join succeeds with a nonempty member identity and positive epoch
    Given the existing streams group's complete description is recorded
    When the broker receives these instance-1 streams requests
      | kind                     |
      | first join               |
      | known member rejoin      |
      | known member heartbeat   |
      | unknown member heartbeat |
      | temporary leave          |
      | permanent leave          |
      | absent group first join  |
    Then all seven static requests return the complete unsupported-static-membership refusal
    When the existing and absent streams groups are described after the refusals
    Then the complete existing streams description is unchanged and static-absent was never created
