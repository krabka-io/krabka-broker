Feature: tests jvm acceptance cli group output

  Scenario: tests: the reset table keeps only numeric rows and sorts them
    Then jvm acceptance cli group output tests: the reset table keeps only numeric rows and sorts them

  Scenario: tests: a table printed in another order parses to the same rows
    Then jvm acceptance cli group output tests: a table printed in another order parses to the same rows

  Scenario: tests: lines that are not rows are not rows
    Then jvm acceptance cli group output tests: lines that are not rows are not rows

  Scenario: tests: the export csv parses in both of the shapes the tool writes
    Then jvm acceptance cli group output tests: the export csv parses in both of the shapes the tool writes

  Scenario: tests: a quoted export row reads the same as a plain one
    Then jvm acceptance cli group output tests: a quoted export row reads the same as a plain one

  Scenario: tests: a doubled quote inside a field reads back as one
    Then jvm acceptance cli group output tests: a doubled quote inside a field reads back as one

  Scenario: tests: every kafka exception in a tool dump is found once
    Then jvm acceptance cli group output tests: every kafka exception in a tool dump is found once

  Scenario: tests: delete reports reduce to one outcome per group
    Then jvm acceptance cli group output tests: delete reports reduce to one outcome per group
