Feature: Client metrics subscriptions select a client's metrics and push interval

  Rule: A client that no subscription matches receives no metrics at the default interval

    Scenario: A client receives no metrics when the cluster has no subscriptions
      When client svc-1 running apache-kafka-java asks for its metrics subscription
      Then the client receives no metrics every 300000 milliseconds

  Rule: A subscription without a match selector applies to every client

    Scenario: A subscription without a selector gives its metrics to any client
      Given a subscription s1 selects metrics a.
      When client svc-1 running apache-kafka-java asks for its metrics subscription
      Then the client receives metrics a. every 300000 milliseconds

  Rule: The push interval is the smallest matched interval and never above the default

    Scenario Outline: One subscription sets the push interval up to the default
      Given a subscription s1 pushes metrics a. every <interval> milliseconds
      When client svc-1 running apache-kafka-java asks for its metrics subscription
      Then the client receives metrics a. every <pushed> milliseconds

      Examples:
        | case                                | interval | pushed |
        | an interval above the default       | 600000   | 300000 |
        | an interval below the default       | 60000    | 60000  |

    Scenario: The shorter interval of two matched subscriptions wins
      Given a subscription s1 pushes metrics a. every 60000 milliseconds
      And a subscription s2 pushes metrics b. every 30000 milliseconds
      When client svc-1 running apache-kafka-java asks for its metrics subscription
      Then the client receives metrics a., b. every 30000 milliseconds

    Scenario: A subscription with an empty metrics list still shortens the interval
      Given a subscription s1 pushes metrics a. every 60000 milliseconds
      And a subscription s2 pushes an empty metrics list every 1000 milliseconds
      When client svc-1 running apache-kafka-java asks for its metrics subscription
      Then the client receives metrics a. every 1000 milliseconds

  Rule: The client receives the union of the matched subscriptions' metrics

    Scenario: Two subscriptions' prefixes are united
      Given a subscription s1 selects metrics a.
      And a subscription s2 selects metrics b.
      When client svc-1 running apache-kafka-java asks for its metrics subscription
      Then the client receives metrics a., b. every 300000 milliseconds

    Scenario: A prefix that two subscriptions select is received once
      Given a subscription s1 selects metrics a.
      And a subscription s2 selects metrics a.
      When client svc-1 running apache-kafka-java asks for its metrics subscription
      Then the client receives metrics a. every 300000 milliseconds

    Scenario: A subscription to every metric absorbs the other prefixes
      Given a subscription s1 selects metrics a.
      And a subscription s2 selects metrics *
      When client svc-1 running apache-kafka-java asks for its metrics subscription
      Then the client receives metrics * every 300000 milliseconds

    Scenario: An empty metrics list selects no metric
      Given a subscription s1 pushes an empty metrics list every 60000 milliseconds
      When client svc-1 running apache-kafka-java asks for its metrics subscription
      Then the client receives no metrics every 60000 milliseconds

    Scenario: A subscription without a metrics key selects no metric
      Given a subscription s1 sets only a push interval of 60000 milliseconds
      When client svc-1 running apache-kafka-java asks for its metrics subscription
      Then the client receives no metrics every 60000 milliseconds

  Rule: A match selector must match the whole client attribute it names

    Scenario Outline: A selector that matches the client subscribes it
      Given a subscription s1 selects metrics *
      And the subscription s1 matches clients by <selector>
      When client <client> running <software> asks for its metrics subscription
      Then the client receives metrics * every 300000 milliseconds

      Examples:
        | case                                       | selector                     | client | software          |
        | an alternation naming the whole client id  | client_id=app\|app-1         | app-1  | apache-kafka-java |
        | a software name selector                   | client_software_name=unknown | svc-1  | unknown           |
        | a repeated selector keeps its last pattern | client_id=a,client_id=b      | b      | apache-kafka-java |

    Scenario Outline: A selector that does not match the client leaves it unsubscribed
      Given a subscription s1 selects metrics *
      And the subscription s1 matches clients by <selector>
      When client <client> running <software> asks for its metrics subscription
      Then the client receives no metrics every 300000 milliseconds

      Examples:
        | case                         | selector                          | client | software          |
        | a prefix of the client id    | client_id=app                     | app-1  | apache-kafka-java |
        | another software name        | client_software_name=kafka-python | svc-1  | apache-kafka-java |

    Scenario Outline: Each client attribute selector tests its own attribute
      Given a subscription s1 selects metrics *
      And the subscription s1 matches clients by <selector>
      When a client with <attribute> <value> asks for its metrics subscription
      Then the client receives <received> every 300000 milliseconds

      Examples:
        | case                     | selector                       | attribute               | value    | received   |
        | the software version     | client_software_version=3.9.0  | client_software_version | 3.9.0    | metrics *  |
        | another software version | client_software_version=3.9.0  | client_software_version | 4.0.0    | no metrics |
        | the source address       | client_source_address=10.0.0.5 | client_source_address   | 10.0.0.5 | metrics *  |
        | another source address   | client_source_address=10.0.0.5 | client_source_address   | 10.0.0.6 | no metrics |
        | the source port          | client_source_port=5556        | client_source_port      | 5556     | metrics *  |
        | another source port      | client_source_port=5556        | client_source_port      | 9092     | no metrics |

  Rule: A subscription whose selector does not compile matches no client

    Scenario: An unparsable selector neither subscribes the client nor shortens its interval
      Given a subscription s1 pushes metrics * every 1000 milliseconds
      And the subscription s1 matches clients by client_id=(
      When client svc-1 running apache-kafka-java asks for its metrics subscription
      Then the client receives no metrics every 300000 milliseconds
