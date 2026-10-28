Feature: Fetch session epoch controls full and incremental classification
  Reconnecting and final fetches close an existing session; uncached incremental
  fetches fail without allocating cache state.

  Scenario: Reconnect closes the old session before opening a new one
    Given a fetch request names an existing cached session
    When the name-based fetch is classified with session epoch 0
    Then the fetch is classified as opening a new session
    And the fetch session cache contains no sessions

  Scenario: An incremental fetch cannot find uncached session zero
    Given a fetch request names uncached session id 0
    When the name-based fetch is classified with session epoch 5
    Then the fetch fails with FETCH_SESSION_ID_NOT_FOUND
    And the fetch session cache contains no sessions

  Scenario: A final fetch naming an unknown session remains sessionless
    Given a fetch request names uncached session id 12345
    When the name-based fetch is classified with session epoch -1
    Then the fetch is classified as sessionless
    And the fetch session cache contains no sessions

  Scenario: A final fetch closes the named existing session
    Given a fetch request names an existing cached session
    When the name-based fetch is classified with session epoch -1
    Then the fetch is classified as sessionless
    And the fetch session cache contains no sessions

  Scenario: Session zero at the initial epoch opens a new session
    Given a fetch request names uncached session id 0
    When the name-based fetch is classified with session epoch 0
    Then the fetch is classified as opening a new session
    And the fetch session cache contains no sessions
