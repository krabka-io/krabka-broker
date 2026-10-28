@docker
Feature: Resource permissions remain independent of authentication and other grants
  Scenario: Valid credentials do not grant topic writes
    Given bob authenticates successfully but only alice has topic Read and Write grants
    When bob's ordinary producer submits an unauthorized record to foo
    Then the stock client logs TopicAuthorizationException

  Scenario: Topic read permission does not grant consumer group access
    Given alice can read foo but has no permission on consumer group cg other
    When alice's consumer joins cg other to read foo
    Then the stock client logs GroupAuthorizationException
