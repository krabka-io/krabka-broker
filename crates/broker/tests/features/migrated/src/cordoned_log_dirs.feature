Feature: Cordoned directories resolve to configured broker log paths
  An administrator can select existing directories or all directories using a wildcard.

  Background:
    Given the broker log directories are /d1 and /d2

  Scenario Outline: Resolve a legal cordoning selection
    Given cordoned.log.dirs is JSON <value>
    When the cordoned log directories are resolved
    Then the ordered cordoned directories are JSON <paths>

    Examples:
      | value | paths |
      | "" | [] |
      | "  " | [] |
      | "/d1" | ["/d1"] |
      | "/d2 , /d1" | ["/d1","/d2"] |
      | "/d1,/d1" | ["/d1"] |
      | "*" | ["/d1","/d2"] |

  Scenario Outline: Refuse an invalid cordoning selection
    Given cordoned.log.dirs is JSON <value>
    When the cordoned log directories are resolved
    Then the cordoning request fails with <message>

    Examples:
      | value | message |
      | "/d1," | Configuration 'cordoned.log.dirs' values must not be empty. |
      | "*,/d1" | requirement failed: When cordoned.log.dirs is set to *, it must not contain other values |
      | "/nope" | requirement failed: All entries in cordoned.log.dirs must be present in log.dirs or log.dir. Missing entries : /nope |
      | "/x,/d1,/y" | requirement failed: All entries in cordoned.log.dirs must be present in log.dirs or log.dir. Missing entries : /x, /y |
