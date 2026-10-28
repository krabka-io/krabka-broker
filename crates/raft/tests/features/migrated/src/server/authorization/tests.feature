Feature: src server authorization tests

  Scenario Outline: each controller api needs the kafka operation
    Then server authorization tests: each controller api needs the kafka operation for <example>

    Examples:
      | example                   |
      | fetch                     |
      | vote                      |
      | begin quorum epoch        |
      | end quorum epoch          |
      | fetch snapshot            |
      | controller registration   |
      | update raft voter         |
      | assign replicas to dirs   |
      | submit change             |
      | metadata fetch            |
      | delegation token mutation |
      | add raft voter            |
      | remove raft voter         |
      | describe cluster          |
      | describe quorum           |
      | api versions              |
      | create topics             |
      | envelope                  |
      | broker registration       |
      | broker heartbeat          |

  Scenario: fetch and quorum refusals are kafka error responses
    Then server authorization tests: fetch and quorum refusals are kafka error responses

  Scenario: admin and private refusals are kafka error responses
    Then server authorization tests: admin and private refusals are kafka error responses
