Feature: A topic freeze preserves its existing data
  Scenario: delete records and delete topics refuse on a frozen topic
    Given orders is frozen after two records and control also contains two records
    When the administrator trims orders through offset one
    Then the frozen trim is refused and orders still spans offsets zero through two
    When the administrator deletes topic orders
    Then the frozen deletion is refused and orders still exists
    When the administrator trims control through offset one
    Then the control topic deletion operation succeeds
    When the administrator deletes topic control
    Then the control topic deletion operation succeeds
