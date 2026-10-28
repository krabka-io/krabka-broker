@docker
Feature: Scheduled delivery works with stock producer timestamps and consumers
  Scenario: Stock JVM clients schedule and consume records without client changes
    Given one broker hosts a scheduled topic and an immediate delivery control topic
    When a stock JVM producer schedules the same timestamped payload on both topics and polls before and after it is due
    Then only the scheduled topic withholds the record before its timestamp and both topics return the original timestamped payload afterward
    When an unconfigured stock console consumer reads the now due scheduled record
    Then the console consumer receives the scheduled payload
