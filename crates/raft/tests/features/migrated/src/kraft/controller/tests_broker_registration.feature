Feature: Broker registration transitions use Kafka metadata records

  Rule: A broker's epoch is the metadata log offset of its registration

    Scenario: A registering broker takes the offset of its registration as its epoch
      Given a single voter controller leads the broker registration log
      When broker 7 performs registration transition register
      Then the broker epoch is the offset its registration was appended at

    Scenario: A broker that registers again takes the offset of its new registration
      Given a single voter controller leads the broker registration log
      And broker 7 has made registration transitions register, unfence
      When broker 7 performs registration transition register
      Then the broker epoch is the offset its registration was appended at

  Rule: Each registration transition writes the Kafka record for it

    Scenario: A registering broker is recorded as newly fenced
      Given a single voter controller leads the broker registration log
      When broker 7 performs registration transition register
      Then Kafka records a newly fenced broker with its assigned epoch

    Scenario: An unfenced broker rejoins service
      Given a single voter controller leads the broker registration log
      And broker 7 has made registration transitions register
      When broker 7 performs registration transition unfence
      Then Kafka records fencing -1 shutdown 0 at schema version 0 and the broker is fenced false shutting down false

    Scenario: A broker in controlled shutdown is recorded at schema version 1
      Given a single voter controller leads the broker registration log
      And broker 7 has made registration transitions register, unfence
      When broker 7 performs registration transition shutdown
      Then Kafka records fencing 0 shutdown 1 at schema version 1 and the broker is fenced false shutting down true

    Scenario: A broker fenced during controlled shutdown stays shutting down
      Given a single voter controller leads the broker registration log
      And broker 7 has made registration transitions register, unfence, shutdown
      When broker 7 performs registration transition fence
      Then Kafka records fencing 1 shutdown 0 at schema version 0 and the broker is fenced true shutting down true

    Scenario: An unregistered broker is removed at its assigned epoch
      Given a single voter controller leads the broker registration log
      And broker 7 has made registration transitions register, unfence, shutdown, fence
      When broker 7 performs registration transition unregister
      Then Kafka records unregistration at the assigned epoch and removes broker 7
