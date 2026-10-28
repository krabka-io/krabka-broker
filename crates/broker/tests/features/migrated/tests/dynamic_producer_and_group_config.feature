Feature: Dynamic runtime configuration reaches producer expiry and consumer assignment scheduling
  Scenario Outline: a dynamic producer id expiration reaches the running expiry sweep
    Given a broker sweeps expired producers every 50 milliseconds
    And topic <topic> contains a current timestamped idempotent batch
    When DescribeProducers is read after 300 milliseconds of static-window sweeps
    Then the only active producer is the one that appended the batch
    When runtime config producer.id.expiration.ms on <scope> is set to 1
    Then the full runtime config alteration response is successful
    When DescribeProducers is polled until the shortened expiry window applies
    Then the active producer list is empty
    When runtime config producer.id.expiration.ms on <scope> is deleted
    Then the full runtime config alteration response is successful
    Examples:
      | scope | topic |
      | default | expiring-clusterdefault |
      | this-broker | expiring-thisbroker |

  Scenario: a dynamic consumer assignment interval holds back the next assignment
    Given a ready consumer group broker has interval-first and interval-second single partition topics
    When runtime config group.consumer.assignment.interval.ms on default is set to 2000
    Then the full runtime config alteration response is successful
    When the member joins g-interval at epoch 0 subscribed to interval-first
    Then the complete first heartbeat assigns interval-first partition 0 at epoch 2 and interval 5000
    When the member at epoch 2 subscribes to both interval topics
    Then the complete heartbeat remains at epoch 2 with no assignment and interval 5000
    When heartbeats continue until the deferred target assignment is released
    Then at least 2000 milliseconds elapsed and the complete epoch 3 heartbeat assigns both partition 0 topics

  Scenario Outline: an assignment interval above the broker maximum is refused
    Given a default broker is ready for assignment interval validation
    When runtime config group.<protocol>.assignment.interval.ms on <scope> is set to 15001
    Then the full alteration response is error 42 with message <message>
    Examples:
      | scope | protocol | message |
      | default | consumer | group.consumer.assignment.interval.ms must be less than or equal to group.consumer.max.assignment.interval.ms |
      | this-broker | share | group.share.assignment.interval.ms must be less than or equal to group.share.max.assignment.interval.ms |
      | default | streams | group.streams.assignment.interval.ms must be less than or equal to group.streams.max.assignment.interval.ms |
