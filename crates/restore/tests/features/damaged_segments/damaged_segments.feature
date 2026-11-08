Feature: A restore continues past damaged segments, never past an unreadable archive

  A restore run with --continue-on-corrupt skips a segment whose archived
  copy is damaged, names it in the report, and restores the rest. A segment
  the restore cannot read because the archive did not answer is not damaged:
  skipping it would report intact records as lost. So the restore stops, as
  it does without the flag, and a later run can read the segment.

  Rule: A segment whose archived copy is lost is skipped

    Scenario: A segment whose log the archive lost is skipped
      Given a partition has an archived segment
      And the archive answers every read with NoSuchKey
      When an operator restores the archive, continuing past corrupt segments
      Then the report names the segment as skipped

  Rule: An archive that does not answer stops the restore

    Scenario Outline: An archive that refuses every read stops the restore
      Given a partition has an archived segment
      And the archive answers every read with <fault>
      When an operator attempts to restore the archive, continuing past corrupt segments
      Then the restore stops because the archive cannot be read

      Examples:
        | case                 | fault        |
        | a throttled archive  | SlowDown     |
        | a refused credential | AccessDenied |
