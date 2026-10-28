Feature: tests diskless e2e cold read

  Scenario: trimmed diskless offsets are served from the object store
    Then diskless e2e cold read: trimmed diskless offsets are served from the object store

  Scenario: deleted diskless records leave the object store unreadable
    Then diskless e2e cold read: deleted diskless records leave the object store unreadable
