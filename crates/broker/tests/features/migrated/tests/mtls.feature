Feature: Certificate principal mapping distinguishes trusted identities unmappable identities and absent certificates
  Scenario: A trusted client certificate authorizes its Subject DN superuser
    Given a TLS listener uses client authentication mode required and trusts the fixture client CA
    And the fixture certificate Subject DN is the only configured superuser
    And the configured TLS broker is running
    And the TLS client pins the server and presents the trusted fixture certificate
    When the client completes TLS and requests creation of topic mtls-smoke
    Then the broker responds successfully through the certificate DN superuser session

  Scenario: A trusted certificate matching no mapping rule is disconnected
    Given a TLS listener uses client authentication mode required and trusts the fixture client CA
    And the fixture certificate Subject DN is the only configured superuser
    And the listener accepts only these certificate principal mapping rules
      | rule |
      | RULE:^CN=(.*?),OU=ServiceUsers.*$/$1/ |
    And the configured TLS broker is running
    And the TLS client pins the server and presents the trusted fixture certificate
    And the client bounds its request wait to ten seconds
    When the client completes TLS and requests creation of topic unmappable-dn
    Then the unmappable certificate connection closes without any Kafka response

  Scenario: An optional certificate listener serves clients that present no certificate
    Given a TLS listener uses client authentication mode optional and trusts the fixture client CA
    And the fixture certificate Subject DN is the only configured superuser
    And the configured TLS broker is running
    And the TLS client pins the server and presents no client certificate
    And the client bounds its request wait to ten seconds
    When the client completes TLS and requests creation of topic anonymous-tls
    Then the broker serves the certificate less ANONYMOUS session successfully
