Feature: src meta properties

  Scenario Outline: tests: the file is the one kafka writes
    Then meta properties tests: the file is the one kafka writes for <example>

    Examples:
      | example              |
      | with directory id    |
      | without directory id |

  Scenario Outline: tests: a bad file gets kafkas message
    Then meta properties tests: a bad file gets kafkas message for <example>

    Examples:
      | example                                |
      | bad version                            |
      | unknown version                        |
      | unsupported zero                       |
      | missing version                        |
      | old json                               |
      | no node or cluster                     |
      | node trailing space                    |
      | node overflow                          |
      | bad directory precedes cluster missing |
      | no cluster                             |
      | invalid cluster                        |
      | malformed escape                       |

  Scenario Outline: tests: verify ensemble matches kafka
    Then meta properties tests: verify ensemble matches kafka for <example>

    Examples:
      | example                     |
      | nothing formatted           |
      | one cluster and node        |
      | set supplies node id        |
      | cluster id disagrees        |
      | node id disagrees           |
      | directory node ids disagree |
      | reserved directory id       |
      | shared directory id         |
      | missing directory ids       |
