Feature: Live classic membership prevents an incompatible streams conversion

  Scenario Outline: classic group with live member rejects streams heartbeat
    Given an upgrade broker holds one-partition topic in2 for group <group>
    And the classic upgrade group retains a live member with instance ID <instance>
    When a streams first-join heartbeat targets the occupied classic group
    Then the occupied classic group returns the complete not-a-streams-group refusal
    And the occupied classic group retains its single live member

    Examples:
      | group | instance   |
      | g2    | absent     |
      | g3    | instance-1 |
