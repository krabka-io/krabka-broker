Feature: Super user privilege does not bypass token owner-or-renewer authorization
  Scenario: super user renews and expires only as a listed renewer
    Given a token broker has super user admin admin-pw and ordinary alice alice-pw
    And admin authenticates with PLAIN password admin-pw
    When admin mints token unlisted for owner alice with renewers none and lifetime -1
    Then token creation reports error 0
    And token unlisted is committed with owner User alice requester User admin and renewers none
    When admin mints token listed for owner alice with renewers admin and lifetime -1
    Then token creation reports error 0
    And token listed is committed with owner User alice requester User admin and renewers admin
    When admin renews token unlisted for -1 milliseconds
    Then renewal is refused with owner mismatch error 63
    When admin expires token unlisted with period -1
    Then expiry is refused with owner mismatch error 63
    And token unlisted retains its original expiry timestamp
    When admin renews token listed for -1 milliseconds
    Then token renewal reports error 0
    And token listed renewed expiry never moves backward or beyond the maximum timestamp
    When admin expires token listed with period -1
    Then token expiry reports error 0
    And token listed has disappeared from the committed image
