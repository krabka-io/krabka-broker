Feature: A SASL connection state admits only the requests Kafka serves in it

  Rule: A connection may send only the next step of its SASL exchange until it has authenticated

    Scenario Outline: A connection admits only the requests of its SASL state
      Given a connection whose SASL state is <state>
      When the connection sends each of SaslHandshake, ApiVersions, SaslAuthenticate, Produce, Metadata
      Then the broker admits <admitted>

      Examples:
        | case                                     | state               | admitted                                                    |
        | no handshake has run yet                 | anonymous           | SaslHandshake,ApiVersions                                   |
        | a handshake chose a mechanism            | negotiating         | SaslAuthenticate                                            |
        | a reauthentication exchange is in flight | reauthenticating    | SaslAuthenticate                                            |
        | the exchange completed                   | scram-authenticated | SaslHandshake,ApiVersions,SaslAuthenticate,Produce,Metadata |

    Scenario: A reauthentication that switched its mechanism is refused every request
      Given a connection whose SASL state is reauth-bad-mechanism
      When the connection sends each of SaslHandshake, ApiVersions, SaslAuthenticate, Produce, Metadata
      Then the broker refuses every request

  Rule: Only a user other than ANONYMOUS who did not authenticate with a delegation token may use the delegation-token APIs

    Scenario Outline: An authenticated user may use the delegation-token APIs
      Given a connection whose SASL state is <state>
      When the connection sends each of CreateDelegationToken, RenewDelegationToken, ExpireDelegationToken, DescribeDelegationToken
      Then each delegation-token API is allowed

      Examples:
        | case            | state               |
        | a SCRAM session | scram-authenticated |
        | an mTLS session | mtls-authenticated  |

    Scenario Outline: A connection without such a user is refused the delegation-token APIs
      Given a connection whose SASL state is <state>
      When the connection sends each of CreateDelegationToken, RenewDelegationToken, ExpireDelegationToken, DescribeDelegationToken
      Then each delegation-token API is rejected

      Examples:
        | case                                     | state                   |
        | no handshake has run yet                 | anonymous               |
        | a handshake chose a mechanism            | negotiating             |
        | a reauthentication exchange is in flight | reauthenticating        |
        | an authenticated ANONYMOUS principal     | authenticated-anonymous |
        | a session minted from a delegation token | token-authenticated     |
