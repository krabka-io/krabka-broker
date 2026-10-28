Feature: src remote log manager quota tests

  Scenario Outline: the quota follows kafkas simple rate
    Then remote log manager quota tests: the quota follows kafkas simple rate for <example>

    Examples:
      | example                                         |
      | an idle quota allows                            |
      | a burst over the bound throttles for the excess |
      | the window widens with elapsed time             |
      | a rate at the bound is allowed                  |
      | a sample older than the whole window is purged  |
      | later samples add to the rate                   |
      | raising the bound releases the recorded bytes   |
      | lowering the bound throttles the recorded bytes |
      | acquire records only what it admits             |
