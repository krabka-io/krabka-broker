Feature: tests cert hot reload

  Scenario: reload tls swaps served cert
    Then cert hot reload: reload tls swaps served cert

  Scenario: periodic watcher reloads on mtime change
    Then cert hot reload: periodic watcher reloads on mtime change
