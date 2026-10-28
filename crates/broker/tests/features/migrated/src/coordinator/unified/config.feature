Feature: Default consumer-group admission capacity
  An operator who leaves the member limit unset can run a group larger than
  200 members, including a classic group upgrading to the consumer protocol.

  Scenario: Consumer groups inherit Kafka's integer maximum member limit
    Given no consumer-group member limit has been overridden
    When the consumer coordinator reads its maximum group size
    Then the member limit is 2147483647 and permits more than 200 members
