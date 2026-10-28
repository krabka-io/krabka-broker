Feature: tests api versions differential divergence

  Scenario: tests: join covers both tables and labels each key
    Then api versions differential divergence tests: join covers both tables and labels each key

  Scenario: tests: every recorded intent is non empty and keyed once
    Then api versions differential divergence tests: every recorded intent is non empty and keyed once

  Scenario: tests: a served range divergence is recorded on a matching advertised range
    Then api versions differential divergence tests: a served range divergence is recorded on a matching advertised range

  Scenario: tests: a served range divergence whose advertised range moves panics
    Then api versions differential divergence tests: a served range divergence whose advertised range moves panics

  Scenario: tests: an unrecorded range divergence panics
    Then api versions differential divergence tests: an unrecorded range divergence panics

  Scenario: tests: the recorded tables admit no range divergence
    Then api versions differential divergence tests: the recorded tables admit no range divergence

  Scenario: tests: the checked in report is exactly what store writes
    Then api versions differential divergence tests: the checked in report is exactly what store writes

  Scenario: tests: report round trips through json
    Then api versions differential divergence tests: report round trips through json
