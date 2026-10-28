Feature: Authorize named resources and filter undiscoverable topics
  Scenario: metadata silent filter on fetch all
    Given topics t1 and t2 exist but alice may Describe only t1
    When alice requests Metadata for all topics
    Then all-topic Metadata silently includes t1 and excludes t2

  Scenario: metadata explicit deny on named topic
    Given topics t1 and t2 exist but alice may Describe only t1
    When alice requests Metadata explicitly for t2
    Then named Metadata includes only t2 with topic authorization error 29

  Scenario: join group denied without group read acl
    Given alice has no group or transactional grants and ACL authorization is active
    When alice joins cg-1 without a member id
    Then JoinGroup is denied with group authorization error 30
    When alice is granted Read on literal group cg-1
    And alice retries joining cg-1 after the group grant applies
    Then JoinGroup requires a nonempty broker generated member id with error 79
    When alice joins cg-1 with the assigned member id
    Then alice completes the group rebalance without error

  Scenario: init producer id denied without txn acl
    Given alice has no group or transactional grants and ACL authorization is active
    When alice initializes transactional producer tx-1
    Then InitProducerId is denied with transactional id authorization error 53
