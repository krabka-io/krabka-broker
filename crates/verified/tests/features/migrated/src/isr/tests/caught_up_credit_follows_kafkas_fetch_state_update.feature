Feature: src isr tests caught up credit follows kafkas fetch state update

  Scenario Outline: caught up credit follows kafkas fetch state update
    Then isr tests caught up credit follows kafkas fetch state update: caught up credit follows kafkas fetch state update for <example>

    Examples:
      | example                             |
      | reaches current log end             |
      | reaches previous log end            |
      | below both log ends                 |
      | first fetch after leadership change |

  Scenario Outline: isr maintenance keeps caught up followers and drops the rest
    Then isr tests caught up credit follows kafkas fetch state update: isr maintenance keeps caught up followers and drops the rest for <example>

    Examples:
      | example               |
      | leader                |
      | reassigned away       |
      | idle at leader end    |
      | recently caught up    |
      | fetching behind       |
      | stopped fetching      |
      | out of sync caught up |
      | old catchup fetching  |
      | old catchup stopped   |
