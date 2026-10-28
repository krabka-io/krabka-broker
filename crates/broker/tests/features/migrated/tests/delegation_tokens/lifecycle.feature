Feature: Delegation token lifecycle authenticates renews describes and expires the actual token
  Scenario: delegation token lifecycle end to end
    Given a token broker has alice wonderland and bob builder with a 7 day ceiling and 24 hour renew period
    And alice authenticates with PLAIN password wonderland
    When alice mints token lifecycle for owner caller with renewers bob and lifetime -1
    Then token creation reports error 0
    And token lifecycle response names owner User alice requester User alice a token id and 64 byte HMAC
    And token lifecycle initial expiry follows issue time and is strictly before its maximum timestamp
    And token lifecycle is committed with owner User alice requester User alice and renewers bob
    When a new connection authenticates with token lifecycle using SCRAM SHA 256 and base64 HMAC
    Then the token SCRAM connection authenticates successfully
    When the token authenticated connection tries to mint another token
    Then token creation reports error 64
    Given bob authenticates with PLAIN password builder
    When bob renews token lifecycle for 2592000000 milliseconds
    Then token renewal reports error 0
    And token lifecycle renewed expiry never moves backward or beyond the maximum timestamp
    When alice describes tokens using owner filter User alice
    Then the successful description contains exactly token lifecycle owned by User alice
    When alice expires token lifecycle with period -1
    Then token expiry reports error 0
    And token lifecycle has disappeared from the committed image
    When all existing sessions disconnect and fresh SCRAM authentication uses expired token lifecycle
    Then the expired token cannot authenticate on the fresh connection
