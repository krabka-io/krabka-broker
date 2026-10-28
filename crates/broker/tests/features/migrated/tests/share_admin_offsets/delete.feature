Feature: tests share admin offsets delete

  Scenario: delete removes topic
    Then share admin offsets delete: delete removes topic

  Scenario: delete rewrites metadata topic absent after restart
    Then share admin offsets delete: delete rewrites metadata topic absent after restart
