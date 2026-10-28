Feature: Only a super user can mint a token owned by another principal
  Scenario: act as super user mints token owned by target
    Given a token broker has super user admin admin-pw and ordinary alice alice-pw
    And admin authenticates with PLAIN password admin-pw
    When admin mints token delegated for owner alice with renewers none and lifetime -1
    Then token creation reports error 0
    And token delegated response names owner User alice requester User admin a token id and 64 byte HMAC
    And token delegated is committed with owner User alice requester User admin and renewers none
    When admin describes tokens using owner filter User admin
    Then the successful description contains exactly token delegated owned by User alice
    And the described token requester is User admin
    When a new connection authenticates with token delegated using SCRAM SHA 256 and base64 HMAC
    Then the token SCRAM connection authenticates successfully
    When the token authenticated connection tries to mint another token
    Then token creation reports error 64

  Scenario: act as non super user rejected with authorization failed
    Given a token broker has alice alice-pw and no super users
    And alice authenticates with PLAIN password alice-pw
    When alice mints token impersonation for owner bob with renewers none and lifetime -1
    Then act-as token creation is refused with authorization error 65
