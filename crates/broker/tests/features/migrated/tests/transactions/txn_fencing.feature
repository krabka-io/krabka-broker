Feature: Producer identities and group membership fence zombie transactions

  Scenario: fenced producer cannot commit
    Given a fencing broker with unstable protocol versions false
    And producer A has acknowledged first in an open shared-tid transaction
    When producer B initializes the same shared-tid transactional identity
    And producer A attempts to commit its acknowledged transaction
    Then producer A's latest commit fails with FencedProducer
    When producer A attempts to commit its acknowledged transaction
    Then producer A's latest commit fails with FencedProducer

  Scenario Outline: init producer id fences a stale producer identity
    Given a fencing broker with unstable protocol versions false
    And the coordinator holds a fresh kip360-tid producer identity
    When InitProducerId supplies identity mode <mode> with producer ID delta <id_delta> and epoch delta <epoch_delta>
    Then InitProducerId returns error <error> and withholds an identity on refusal

    Examples:
      | example                           | mode     | id_delta | epoch_delta | error |
      | no identity supplied              | absent   | 0        | 0           | 0     |
      | the live identity                 | relative | 0        | 0           | 0     |
      | no epoch                          | relative | 0        | -1          | 42    |
      | an unreached epoch                | relative | 0        | 1           | 90    |
      | another producer id               | relative | 1        | 0           | 90    |
      | another producer id and no epoch  | relative | 1        | -1          | 42    |

  Scenario: txn offset commit fences classic generation and member
    Given a fencing broker with unstable protocol versions false
    And a classic consumer has joined fence-g for topic fence-in
    When a transactional offset commit for classic uses generation delta 1 and member current
    Then the latest member offset commit returns error 22
    When a transactional offset commit for classic uses generation delta 0 and member unknown
    Then the latest member offset commit returns error 25
    When a transactional offset commit for classic uses generation delta 0 and member current
    Then the latest member offset commit returns error 0

  Scenario: txn offset commit fences next gen member epoch
    Given a fencing broker with unstable protocol versions true
    And a next-generation member has joined ng-g for topic ng-in
    When a transactional offset commit for next-generation uses generation delta -1 and member current
    Then the latest member offset commit returns error 113
    When a transactional offset commit for next-generation uses generation delta 1 and member current
    Then the latest member offset commit returns error 113
    When a transactional offset commit for next-generation uses generation delta 0 and member current
    Then the latest member offset commit returns error 0
