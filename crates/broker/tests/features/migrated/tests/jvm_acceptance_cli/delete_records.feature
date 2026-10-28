Feature: tests jvm acceptance cli delete records

  Scenario: kafka delete records trims log
    Then jvm acceptance cli delete records: kafka delete records trims log

  Scenario: kafka delete records trim survives a broker restart
    Then jvm acceptance cli delete records: kafka delete records trim survives a broker restart
