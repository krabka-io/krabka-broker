Feature: tests auth handlers pre auth limits

  Scenario: a frame over the sasl receive limit fails the authentication
    Then auth handlers pre auth limits: a frame over the sasl receive limit fails the authentication

  Scenario: the larger request limit returns once the peer authenticated
    Then auth handlers pre auth limits: the larger request limit returns once the peer authenticated

  Scenario: a failed authentication holds its answer for the configured delay
    Then auth handlers pre auth limits: a failed authentication holds its answer for the configured delay

  Scenario: a second api versions before the handshake closes the connection
    Then auth handlers pre auth limits: a second api versions before the handshake closes the connection

  Scenario: a pre auth api versions carries no rebootstrap check
    Then auth handlers pre auth limits: a pre auth api versions carries no rebootstrap check
