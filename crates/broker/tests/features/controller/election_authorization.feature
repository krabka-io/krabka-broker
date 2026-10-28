Feature: Operator leader elections require Cluster Alter authorization

  Scenario: Authenticated users need Cluster Alter for preferred elections
    Given a SASL PLAIN broker grants admin and ANONYMOUS super-user access, authenticates alice without ACLs and holds foo-auth-test partition 0
    When alice requests a preferred election of foo-auth-test partition 0 through SASL PLAIN
    Then every requested partition is refused with CLUSTER_AUTHORIZATION_FAILED code 31
