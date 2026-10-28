Feature: tests alter replica log dirs startup

  Scenario: startup resumes move for existing partition
    Then alter replica log dirs startup: startup resumes move for existing partition

  Scenario: startup cleans up stranded future dir
    Then alter replica log dirs startup: startup cleans up stranded future dir
