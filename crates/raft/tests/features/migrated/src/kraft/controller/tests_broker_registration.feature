Feature: Broker registration transitions use Kafka metadata records
  Scenario: A broker registers, rejoins service, shuts down, and unregisters
    Given a single voter controller leads the broker registration log
    When broker 7 performs registration transition register
    Then Kafka records a newly fenced broker with its assigned epoch
    When broker 7 performs registration transition unfence
    Then Kafka records fencing -1 shutdown 0 at schema version 0 and the broker is fenced false shutting down false
    When broker 7 performs registration transition shutdown
    Then Kafka records fencing 0 shutdown 1 at schema version 1 and the broker is fenced false shutting down true
    When broker 7 performs registration transition fence
    Then Kafka records fencing 1 shutdown 0 at schema version 0 and the broker is fenced true shutting down true
    When broker 7 performs registration transition unregister
    Then Kafka records unregistration at the assigned epoch and removes broker 7
