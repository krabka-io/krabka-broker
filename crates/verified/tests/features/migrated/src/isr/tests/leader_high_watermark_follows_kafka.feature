Feature: src isr tests leader high watermark follows kafka

  Scenario Outline: leader high watermark follows kafka
    Then isr tests leader high watermark follows kafka: leader high watermark follows kafka for <example>

    Examples:
      | example                   |
      | slowest in sync           |
      | leader alone              |
      | under min isr             |
      | exactly at min isr        |
      | caught up outside isr     |
      | not caught up outside isr |
      | fenced outside isr        |
      | unfetched isr member      |
      | never regresses           |
      | log end member            |

  Scenario Outline: isr admission follows kafkas alter partition validation
    Then isr tests leader high watermark follows kafka: isr admission follows kafkas alter partition validation for <example>

    Examples:
      | example                               |
      | up to date shrink                     |
      | newer leader epoch                    |
      | newer partition epoch                 |
      | partition epoch precedes leader epoch |
      | old leader epoch                      |
      | not leader                            |
      | replaced isr                          |
      | unassigned replica                    |
      | drops leader                          |
      | invalid recovery                      |
      | ineligible fenced broker              |
