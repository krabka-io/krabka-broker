Feature: Stock JVM clients authenticate using Kerberos across admin and data APIs

  Scenario: cp kafka gssapi client round trip
    Given a GSSAPI broker backed by the MIT KDC kafka.keytab and the alice keytab JVM client
    When the authenticated JVM admin creates topic krabka-gssapi-itest with one sole-replica partition
    Then the authenticated JVM command succeeds
    When the authenticated JVM producer writes records alpha,bravo,charlie
    Then the authenticated JVM command succeeds
    When the authenticated JVM consumer reads 3 records from partition zero with timeout 15000 ms
    Then the authenticated JVM command succeeds
    And the consumer output contains records alpha,bravo,charlie
