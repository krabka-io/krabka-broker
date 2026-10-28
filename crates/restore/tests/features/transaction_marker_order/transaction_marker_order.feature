Feature: Kafka abort-marker ordering in archived transaction indexes
  Background:
    Given an archived segment containing offsets 100 through 104

  Scenario: Sequential transaction aborts are accepted
    Given archive transaction index entries in marker order
      | start | last | producer |
      | 100   | 101  | 7        |
      | 102   | 103  | 8        |
    When the archived segment and transaction index are verified
    Then the transaction index is accepted without requiring start-offset order

  Scenario: Interleaved transactions may start in the opposite order to their abort markers
    Given archive transaction index entries in marker order
      | start | last | producer |
      | 102   | 103  | 2        |
      | 100   | 104  | 1        |
    When the archived segment and transaction index are verified
    Then the transaction index is accepted without requiring start-offset order

  Scenario: A transaction can start before the segment containing its abort marker
    Given archive transaction index entries in marker order
      | start | last | producer |
      | 40    | 100  | 1000     |
    When the archived segment and transaction index are verified
    Then the transaction index is accepted without requiring start-offset order
