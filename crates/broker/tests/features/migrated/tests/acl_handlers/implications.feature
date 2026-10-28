Feature: Topic operation implications authorize Metadata
  Scenario Outline: Read and Write imply Describe for named Metadata
    Given alice has only a topic <operation> grant for foo
    When alice requests Metadata for foo by name
    Then Metadata contains exactly foo without a topic authorization error
    Examples:
      | operation |
      | Read |
      | Write |
