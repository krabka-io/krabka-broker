@docker
Feature: Stock client versions exchange records through broker format conversions
  Scenario: Legacy clients round trip through both record conversions
    Given a conversion capable broker hosts topic legacy-010-round-trip
    When the legacy console producer writes alpha bravo and charlie
    Then the stock producer completes successfully
    When the legacy console consumer reads three records from the beginning
    Then the stock consumer decodes alpha bravo and charlie

  Scenario: A modern consumer decodes records from the legacy producer
    Given a conversion capable broker hosts topic legacy-010-produce-modern-consume
    When the legacy console producer writes alpha bravo and charlie
    Then the stock producer completes successfully
    When the modern console consumer reads three records from the beginning
    Then the stock consumer decodes alpha bravo and charlie

  Scenario: A legacy consumer decodes records from the modern producer
    Given a conversion capable broker hosts topic modern-produce-legacy-010-consume
    When the modern console producer writes alpha bravo and charlie
    Then the stock producer completes successfully
    When the legacy console consumer reads three records from the beginning
    Then the stock consumer decodes alpha bravo and charlie
