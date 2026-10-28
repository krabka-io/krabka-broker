Feature: tests leader epoch epoch fencing

  Scenario: fenced leader epoch truncates zombie writes
    Then leader epoch epoch fencing: fenced leader epoch truncates zombie writes

  Scenario: unknown leader epoch on metadata lag
    Then leader epoch epoch fencing: unknown leader epoch on metadata lag
