# Quoted <empty> denotes a zero-length Kafka string; an empty created-topic list contains no names.
Feature: Delegation token creation authorization, owner identity and lifetime

  Scenario: Owner construction precedes token credential refusal
    Given a token controller with requester admin authenticated by token, tokens enabled true, super users admin and lifetime ceiling 60000
    And the token request has owner type "null", owner name "alice", lifetime -1 and renewers none
    When the controller handles CreateDelegationToken with a 24-hour renew period
    Then token creation refuses with code -1, owner User:ANONYMOUS, requester User:ANONYMOUS and timestamps -1 without persisting tokens

  Scenario: Owner construction also precedes super-user authorization
    Given a token controller with requester admin authenticated by plain, tokens enabled true, super users admin and lifetime ceiling 60000
    And the token request has owner type "null", owner name "alice", lifetime -1 and renewers none
    When the controller handles CreateDelegationToken with a 24-hour renew period
    Then token creation refuses with code -1, owner User:ANONYMOUS, requester User:ANONYMOUS and timestamps -1 without persisting tokens

  Scenario: Token credentials cannot mint tokens
    Given a token controller with requester alice authenticated by token, tokens enabled true, super users none and lifetime ceiling 60000
    And the token request has owner type "null", owner name "null", lifetime -1 and renewers none
    When the controller handles CreateDelegationToken with a 24-hour renew period
    Then token creation refuses with code 64, owner User:alice, requester User:alice and timestamps -1 without persisting tokens

  Scenario: Token credentials cannot mint for another owner even as super user
    Given a token controller with requester admin authenticated by token, tokens enabled true, super users admin and lifetime ceiling 60000
    And the token request has owner type "User", owner name "alice", lifetime -1 and renewers none
    When the controller handles CreateDelegationToken with a 24-hour renew period
    Then token creation refuses with code 64, owner User:alice, requester User:admin and timestamps -1 without persisting tokens

  Scenario: Anonymous principals cannot mint tokens
    Given a token controller with requester ANONYMOUS authenticated by anonymous, tokens enabled true, super users none and lifetime ceiling 60000
    And the token request has owner type "null", owner name "null", lifetime -1 and renewers none
    When the controller handles CreateDelegationToken with a 24-hour renew period
    Then token creation refuses with code 64, owner User:ANONYMOUS, requester User:ANONYMOUS and timestamps -1 without persisting tokens

  Scenario: Token credential refusal precedes disabled token support
    Given a token controller with requester alice authenticated by token, tokens enabled false, super users none and lifetime ceiling 60000
    And the token request has owner type "null", owner name "null", lifetime -1 and renewers none
    When the controller handles CreateDelegationToken with a 24-hour renew period
    Then token creation refuses with code 64, owner User:alice, requester User:alice and timestamps -1 without persisting tokens

  Scenario: Acting as another owner requires authorization
    Given a token controller with requester bob authenticated by plain, tokens enabled true, super users admin and lifetime ceiling 60000
    And the token request has owner type "User", owner name "alice", lifetime -1 and renewers none
    When the controller handles CreateDelegationToken with a 24-hour renew period
    Then token creation refuses with code 65, owner User:alice, requester User:bob and timestamps -1 without persisting tokens

  Scenario: Act-as authorization precedes disabled token support
    Given a token controller with requester bob authenticated by plain, tokens enabled false, super users none and lifetime ceiling 60000
    And the token request has owner type "Group", owner name "eng", lifetime -1 and renewers none
    When the controller handles CreateDelegationToken with a 24-hour renew period
    Then token creation refuses with code 65, owner Group:eng, requester User:bob and timestamps -1 without persisting tokens

  Scenario: Renewers must be User principals
    Given a token controller with requester alice authenticated by plain, tokens enabled true, super users none and lifetime ceiling 60000
    And the token request has owner type "null", owner name "null", lifetime -1 and renewers User:bob,Group:eng
    When the controller handles CreateDelegationToken with a 24-hour renew period
    Then token creation refuses with code 67, owner User:alice, requester User:alice and timestamps -1 without persisting tokens

  Scenario: Renewer validation precedes disabled token support
    Given a token controller with requester alice authenticated by plain, tokens enabled false, super users none and lifetime ceiling 60000
    And the token request has owner type "null", owner name "null", lifetime -1 and renewers User:bob,Group:eng
    When the controller handles CreateDelegationToken with a 24-hour renew period
    Then token creation refuses with code 67, owner User:alice, requester User:alice and timestamps -1 without persisting tokens

  Scenario: Disabled token support returns controller timestamps
    Given a token controller with requester alice authenticated by plain, tokens enabled false, super users none and lifetime ceiling 60000
    And the token request has owner type "User", owner name "alice", lifetime -1 and renewers none
    When the controller handles CreateDelegationToken with a 24-hour renew period
    Then token creation refuses with code 61, owner User:alice, requester User:alice and timestamps 0 without persisting tokens

  Scenario: Default lifetime below renew period
    Given a token controller with requester alice authenticated by plain, tokens enabled true, super users none and lifetime ceiling 60000
    And the token request has owner type "null", owner name "null", lifetime -1 and renewers none
    When the controller handles CreateDelegationToken with a 24-hour renew period
    Then the minted token belongs to User:alice, names requester User:alice, renewers none, expiry delta 60000 and maximum delta 60000

  Scenario: Zero lifetime takes the ceiling
    Given a token controller with requester alice authenticated by plain, tokens enabled true, super users none and lifetime ceiling 604800000
    And the token request has owner type "null", owner name "null", lifetime 0 and renewers none
    When the controller handles CreateDelegationToken with a 24-hour renew period
    Then the minted token belongs to User:alice, names requester User:alice, renewers none, expiry delta 86400000 and maximum delta 604800000

  Scenario: Negative lifetime takes the ceiling
    Given a token controller with requester alice authenticated by plain, tokens enabled true, super users none and lifetime ceiling 604800000
    And the token request has owner type "null", owner name "null", lifetime -5 and renewers none
    When the controller handles CreateDelegationToken with a 24-hour renew period
    Then the minted token belongs to User:alice, names requester User:alice, renewers none, expiry delta 86400000 and maximum delta 604800000

  Scenario: Requested lifetime is capped by broker ceiling
    Given a token controller with requester alice authenticated by plain, tokens enabled true, super users none and lifetime ceiling 3600000
    And the token request has owner type "null", owner name "null", lifetime 604800000 and renewers none
    When the controller handles CreateDelegationToken with a 24-hour renew period
    Then the minted token belongs to User:alice, names requester User:alice, renewers none, expiry delta 3600000 and maximum delta 3600000

  Scenario: Requested lifetime below ceiling is retained
    Given a token controller with requester alice authenticated by plain, tokens enabled true, super users none and lifetime ceiling 604800000
    And the token request has owner type "null", owner name "null", lifetime 3600000 and renewers none
    When the controller handles CreateDelegationToken with a 24-hour renew period
    Then the minted token belongs to User:alice, names requester User:alice, renewers none, expiry delta 3600000 and maximum delta 3600000

  Scenario: Naming self needs no act-as permission
    Given a token controller with requester alice authenticated by plain, tokens enabled true, super users none and lifetime ceiling 604800000
    And the token request has owner type "User", owner name "alice", lifetime -1 and renewers none
    When the controller handles CreateDelegationToken with a 24-hour renew period
    Then the minted token belongs to User:alice, names requester User:alice, renewers none, expiry delta 86400000 and maximum delta 604800000

  Scenario: Empty owner name resolves to requester
    Given a token controller with requester alice authenticated by plain, tokens enabled true, super users none and lifetime ceiling 604800000
    And the token request has owner type "User", owner name "<empty>", lifetime -1 and renewers none
    When the controller handles CreateDelegationToken with a 24-hour renew period
    Then the minted token belongs to User:alice, names requester User:alice, renewers none, expiry delta 86400000 and maximum delta 604800000

  Scenario: Super user mints for another user
    Given a token controller with requester admin authenticated by plain, tokens enabled true, super users admin and lifetime ceiling 60000
    And the token request has owner type "User", owner name "alice", lifetime -1 and renewers none
    When the controller handles CreateDelegationToken with a 24-hour renew period
    Then the minted token belongs to User:alice, names requester User:admin, renewers none, expiry delta 60000 and maximum delta 60000

  Scenario: Super user retains another principal type
    Given a token controller with requester admin authenticated by plain, tokens enabled true, super users admin and lifetime ceiling 60000
    And the token request has owner type "Group", owner name "eng", lifetime -1 and renewers none
    When the controller handles CreateDelegationToken with a 24-hour renew period
    Then the minted token belongs to Group:eng, names requester User:admin, renewers none, expiry delta 60000 and maximum delta 60000

  Scenario: User renewer is persisted
    Given a token controller with requester alice authenticated by plain, tokens enabled true, super users none and lifetime ceiling 60000
    And the token request has owner type "null", owner name "null", lifetime -1 and renewers User:bob
    When the controller handles CreateDelegationToken with a 24-hour renew period
    Then the minted token belongs to User:alice, names requester User:alice, renewers User:bob, expiry delta 60000 and maximum delta 60000
