Feature: Fetch session epoch controls full and incremental classification

  Rule: A full fetch closes the session it names and is served in full

    Scenario Outline: A full fetch is classified by its epoch alone
      Given a fetch request names <session>
      When the name-based fetch is classified with session epoch <epoch>
      Then the fetch is classified as <classification>
      And the fetch session cache contains no sessions

      Examples:
        | case                                      | session                    | epoch | classification        |
        | a reconnect closes the old session        | an existing cached session | 0     | opening a new session |
        | session zero opens a new session          | uncached session id 0      | 0     | opening a new session |
        | a final fetch closes the named session    | an existing cached session | -1    | sessionless           |
        | a final fetch naming an unknown session   | uncached session id 12345  | -1    | sessionless           |
        | a final fetch without a session           | uncached session id 0      | -1    | sessionless           |

  Rule: An incremental fetch must name a cached session at the epoch it expects

    Scenario Outline: An incremental fetch naming no cached session is refused
      Given a fetch request names uncached session id <id>
      When the name-based fetch is classified with session epoch <epoch>
      Then the fetch fails with FETCH_SESSION_ID_NOT_FOUND
      And the fetch session cache contains no sessions

      Examples:
        | case                            | id    | epoch |
        | session zero is never allocated | 0     | 5     |
        | an unknown session              | 12345 | 1     |

    Scenario: An incremental fetch at a stale epoch is refused
      Given a fetch request names an existing cached session
      When the name-based fetch is classified with session epoch 99
      Then the fetch fails with INVALID_FETCH_SESSION_EPOCH

  Rule: An incremental fetch must use topic IDs exactly when its session does

    Scenario Outline: The Fetch version of an incremental fetch must match its session
      Given a fetch session was opened with topic <addressing>
      When an incremental fetch at epoch <epoch> is classified at Fetch v<version>
      Then the incremental fetch is <outcome>
      And the session next expects epoch <next>

      Examples:
        | case                                 | addressing | epoch | version | outcome                      | next |
        | a name session at the last name read | names      | 1     | 12      | accepted                     | 2    |
        | a name session at the first ID read  | names      | 1     | 13      | FETCH_SESSION_TOPIC_ID_ERROR | 1    |
        | an ID session at a later version     | IDs        | 1     | 17      | accepted                     | 2    |
        | an ID session at a name version      | IDs        | 1     | 12      | FETCH_SESSION_TOPIC_ID_ERROR | 1    |
        | a stale epoch outranks the mismatch  | IDs        | 7     | 12      | INVALID_FETCH_SESSION_EPOCH  | 1    |
