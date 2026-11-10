Feature: Cordoned directories resolve to configured broker log paths

  Rule: cordoned.log.dirs names configured directories or the wildcard

    Scenario Outline: Resolve a legal cordoning selection
      Given the broker log directories are /d1 and /d2
      And cordoned.log.dirs is JSON <value>
      When the cordoned log directories are resolved
      Then the ordered cordoned directories are JSON <paths>

      Examples:
        | case                              | value       | paths         |
        | an empty value cordons nothing    | ""          | []            |
        | a blank value cordons nothing     | "  "        | []            |
        | one directory                     | "/d1"       | ["/d1"]       |
        | spaced entries in config order    | "/d2 , /d1" | ["/d1","/d2"] |
        | a repeated entry counts once      | "/d1,/d1"   | ["/d1"]       |
        | the wildcard cordons every one    | "*"         | ["/d1","/d2"] |

    Scenario Outline: Refuse an invalid cordoning selection
      Given the broker log directories are /d1 and /d2
      And cordoned.log.dirs is JSON <value>
      When the cordoned log directories are resolved
      Then the cordoning request fails with <message>

      Examples:
        | case                          | value       | message |
        | an empty entry                | "/d1,"      | Configuration 'cordoned.log.dirs' values must not be empty. |
        | the wildcard beside an entry  | "*,/d1"     | requirement failed: When cordoned.log.dirs is set to *, it must not contain other values |
        | an unconfigured directory     | "/nope"     | requirement failed: All entries in cordoned.log.dirs must be present in log.dirs or log.dir. Missing entries : /nope |
        | every missing entry is listed | "/x,/d1,/y" | requirement failed: All entries in cordoned.log.dirs must be present in log.dirs or log.dir. Missing entries : /x, /y |

  Rule: A dynamic cordoned.log.dirs overrides the static one and an unresolvable update keeps the set in force

    Scenario Outline: The dynamic value decides the cordoned directories
      Given a broker with log directories /d1 and /d2 starts with cordoned.log.dirs /d2
      And its dynamic cordoned.log.dirs has been set to /d1
      When its dynamic cordoned.log.dirs becomes <dynamic>
      Then the broker cordons <cordoned>

      Examples:
        | case                                 | dynamic | cordoned |
        | a directory the broker does not have | /nope   | /d1      |
        | the wildcard                         | *       | /d1,/d2  |
        | an empty value                       | empty   | nothing  |

    Scenario: A dynamic value wins over the static one
      Given a broker with log directories /d1 and /d2 starts with cordoned.log.dirs /d2
      When its dynamic cordoned.log.dirs becomes /d1
      Then the broker cordons /d1

    Scenario: Removing the dynamic value restores the static one
      Given a broker with log directories /d1 and /d2 starts with cordoned.log.dirs /d2
      And its dynamic cordoned.log.dirs has been set to /d1
      When its dynamic cordoned.log.dirs is removed
      Then the broker cordons /d2

  Rule: A broker takes new replicas unless every one of its directories is cordoned

    Scenario Outline: A broker with an uncordoned directory takes new replicas
      Given a broker registered log directories <registered>
      And the broker reported <cordoned> as cordoned
      When automatic replica placement considers the broker
      Then the broker can take new replicas

      Examples:
        | case                         | registered | cordoned |
        | nothing cordoned             | a,b        | nothing  |
        | one of two cordoned          | a,b        | a        |
        | no directory ids registered  | nothing    | a        |

    Scenario: A broker that has not reported its cordoned directories takes new replicas
      Given a broker registered log directories a,b
      When automatic replica placement considers the broker
      Then the broker can take new replicas

    Scenario: A broker whose every directory is cordoned takes no new replica
      Given a broker registered log directories a,b
      And the broker reported b,a as cordoned
      When automatic replica placement considers the broker
      Then the broker cannot take new replicas

  Rule: A heartbeat changes the registration only when its cordoned set differs as a set

    Scenario Outline: A heartbeat with a different cordoned set is stored
      Given a broker registered log directories a,b
      And the broker reported <stored> as cordoned
      When the broker heartbeats with <reported> cordoned
      Then the controller records <reported> as the broker's cordoned directories

      Examples:
        | case                 | stored  | reported |
        | a newly cordoned one | a       | a,b      |
        | uncordon everything  | a       | nothing  |

    Scenario: The first heartbeat that reports a cordoned set is stored
      Given a broker registered log directories a,b
      When the broker heartbeats with nothing cordoned
      Then the controller records nothing as the broker's cordoned directories

    Scenario: A heartbeat with the same set in another order writes no record
      Given a broker registered log directories a,b
      And the broker reported a,b as cordoned
      When the broker heartbeats with b,a cordoned
      Then the controller records no registration change

    Scenario: A heartbeat from a broker that has not caught up writes no record
      Given a broker registered log directories a,b
      When the broker heartbeats without a cordoned set
      Then the controller records no registration change
