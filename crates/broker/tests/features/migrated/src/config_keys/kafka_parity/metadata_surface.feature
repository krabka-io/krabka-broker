Feature: Complete typed and documented Kafka topic configuration surface
  Registry, DescribeConfigs, and generated documentation share one complete
  captured Kafka key surface with explicitly named broker extensions.

  Scenario: Kafka keys are covered and documented with only the named broker extensions
    Given the captured released and trunk topic-key reference catalogs
    When the topic registry and generated configuration descriptions are queried
    Then no captured Kafka topic key is missing and the only extra keys are:
      | key |
      | unclean.recovery.strategy |
      | krabka.diskless |
      | delivery.mode |
      | delivery.max.delay.ms |
      | delivery.schedule.monotonic |
      | schema.validation.key |
      | schema.validation.value |
      | schema.validation.mode |
      | write.freeze |
    And every captured Kafka topic key is documented
    And every captured topic key retains Kafka's type and its default or a documented divergence
    And each documented default divergence is still an actual divergence
