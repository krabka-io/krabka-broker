Feature: TLS certificate rotation changes the certificate served to fresh clients

  Scenario: reload tls swaps served cert
    Given a TLS broker serving certificate A with watcher interval 0 ms
    When a fresh TLS client pins certificate A
    Then the TLS handshake is accepted
    When a fresh TLS client pins certificate B
    Then the TLS handshake is rejected
    When the certificate and key files are replaced with B after 0 ms
    And the broker explicitly reloads TLS
    Then the explicit reload succeeds
    When a fresh TLS client pins certificate B
    Then the TLS handshake is accepted
    When a fresh TLS client pins certificate A
    Then the TLS handshake is rejected

  Scenario: periodic watcher reloads on mtime change
    Given a TLS broker serving certificate A with watcher interval 100 ms
    When the certificate and key files are replaced with B after 1100 ms
    And fresh B-pinned handshakes retry every 50 ms for at most 5000 ms
    Then the TLS handshake is accepted
