Feature: tests static membership

  Scenario: static rejoin preserves assignment and generation
    Then static membership: static rejoin preserves assignment and generation

  Scenario: second client with same instance id is fenced
    Then static membership: second client with same instance id is fenced

  Scenario: leave group resolves static member by instance id
    Then static membership: leave group resolves static member by instance id
