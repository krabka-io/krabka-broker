Feature: Strict controller mutation quotas refuse a topic only once the bucket is in debt

  Rule: A strict request refuses a topic only once the bucket of rate times window is below zero

    Scenario Outline: Strict requests on a fresh bucket of 110 mutations
      Given a user has a controller mutation rate of 10 per second over a quota window of 11 seconds
      When the user sends strict requests whose topics charge <charges> mutations
      Then the topics are <outcomes>

      Examples:
        | case                                            | charges | outcomes                    |
        | requests that stay inside the bucket            | 50; 5,5 | accepted; accepted,accepted |
        | the topic that crosses zero is still accepted   | 60,60,1 | accepted,accepted,refused   |
        | a request that ends below zero refuses the next | 120; 1  | accepted; refused           |
        | a topic just below zero refuses the rest        | 111,5,5 | accepted,refused,refused    |
