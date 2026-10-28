Feature: src isr tests replica isr eligibility follows kip 841

  Scenario Outline: isr candidates follow kafkas shrink and expand rules
    Then isr tests replica isr eligibility follows kip 841: isr candidates follow kafkas shrink and expand rules for <example>

    Examples:
      | example                   |
      | leader                    |
      | reassigned away           |
      | idle at leader end        |
      | recently caught up        |
      | behind bound              |
      | never fetched             |
      | at high watermark         |
      | short of high watermark   |
      | before epoch start        |
      | unknown epoch start       |
      | fenced follower           |
      | out of sync never fetched |
