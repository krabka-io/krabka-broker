Feature: Replica reassignment requires cluster permission in addition to authentication
  Scenario: A valid PLAIN identity without cluster Alter cannot change replica assignments
    Given a SASL PLAIN broker grants admin superuser rights and authenticates these users
      | username | password |
      | admin | admin-secret |
      | alice | alice-secret |
    And User admin has Read on topic __compat_shim_disable__ while alice has no ACL grants
    And the superuser has created topic foo
    When user alice password alice-secret requests these replica assignments while authorization settles
      | topic | partition | replicas |
      | foo | 0 | 1 |
    Then the reassignment response contains exactly these cluster authorization failures
      | topic | partition | code |
      | foo | 0 | 31 |
