@docker
Feature: Compiled Streams topologies produce committed counts and compatible internal topic configuration
  Scenario: A classic protocol Streams topology writes committed counts and configures its internal topics
    Given a broker is ready for the compiled windowed count Streams application
    And Kafka 3.5 runs the application with the classic group protocol
    And application krabka-streams-classic reads topic krabka-streams-classic-in and writes topic krabka-streams-classic-out
    And the windowed count application is compiled against Kafka 3.5 for Java eleven
    When the compiled application runs with exactly once version two processing
    Then the application reports STREAMSPROBE OK
    When the operator reads the committed sink and inspects the application's repartition and window changelog topics
    Then the committed sink contains the running window counts in this exact order
      | count |
      | alpha:1 |
      | alpha:2 |
      | beta:1 |
      | alpha:3 |
      | beta:2 |
    And exactly one of each internal topic exists and its config report includes
      | topic | config |
      | krabka-streams-classic-shuffle-repartition | retention.ms=-1 |
      | krabka-streams-classic-shuffle-repartition | message.timestamp.type=CreateTime |
      | krabka-streams-classic-krabka-counts-changelog | cleanup.policy=compact,delete |
      | krabka-streams-classic-krabka-counts-changelog | message.timestamp.type=CreateTime |

  @allow_skipped
  Scenario: A streams protocol topology writes committed counts when its cross compiled class loads
    Given a broker is ready for the compiled windowed count Streams application
    And Kafka 4.3 runs the application with the streams group protocol and streams version one finalized
    And application krabka-streams-kip1071 reads topic krabka-streams-kip1071-in and writes topic krabka-streams-kip1071-out
    And the windowed count application is compiled against Kafka 3.5 for Java eleven
    When the compiled application runs with exactly once version two processing
    Then the application reports STREAMSPROBE OK
    When the operator reads the committed sink and inspects the application's repartition and window changelog topics
    Then the committed sink contains the running window counts in this exact order
      | count |
      | alpha:1 |
      | alpha:2 |
      | beta:1 |
      | alpha:3 |
      | beta:2 |
    And exactly one of each internal topic exists and its config report includes
      | topic | config |
      | krabka-streams-kip1071-shuffle-repartition | retention.ms=-1 |
      | krabka-streams-kip1071-shuffle-repartition | message.timestamp.type=CreateTime |
      | krabka-streams-kip1071-krabka-counts-changelog | cleanup.policy=compact,delete |
      | krabka-streams-kip1071-krabka-counts-changelog | message.timestamp.type=CreateTime |
