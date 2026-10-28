Feature: Replica destination identity and future directory length

  Scenario Outline: Exact path spelling and future directory length determine refusal
    Given an online configured log directory with a symbolic link and no hosted orders partition
    And the replica move names destination <destination> and topic <topic>
    When AlterReplicaLogDirs version 2 processes the destination
    Then the complete replica move response has partition error <error>

    Examples:
      | destination | topic | error |
      | configured | orders | 9 |
      | configured/ | orders | 57 |
      | configured/. | orders | 57 |
      | configured/../basename | orders | 57 |
      | relative-configured | orders | 57 |
      | symlink | orders | 57 |
      | configured | 213-times-t | 9 |
      | configured | 214-times-t | 17 |
      | configured/ | 214-times-t | 17 |
