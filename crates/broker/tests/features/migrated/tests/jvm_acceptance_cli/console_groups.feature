@docker
Feature: Stock console consumers participate in consumer group coordination
  Scenario: Default assignment consumes all records
    Given topic krabka-broker-grp-itest has 1 partitions containing console values x,y,z
    When a default console consumer joins group krabka-acceptance-group
    Then the group consumer emits values x,y,z

  Scenario: Static membership consumes records and retains the subscription
    Given topic krabka-broker-static-itest has 1 partitions containing console values a,b,c
    When a static console consumer joins group krabka-static-grp with instance client-static-1
    Then the group consumer emits values a,b,c
    When the operator describes group krabka-static-grp
    Then the group report includes its subscribed topic

  Scenario: Cooperative sticky assignment consumes all records
    Given topic coop-jvm has 3 partitions containing console values alpha,bravo,charlie
    When a cooperative sticky consumer joins group coop-jvm-group
    Then the group consumer emits values alpha,bravo,charlie
