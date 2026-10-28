Feature: Runtime configuration tracks Kafka static key provenance across overlays

  Scenario: An empty runtime table records no keys
    Given a runtime configuration file contains:
      """
      [runtime]
      """
    When the runtime files are applied in order to a default broker
    Then the supplied Kafka configuration key origins are JSON []

  Scenario: A runtime-only cleaner setting has no Kafka key
    Given a runtime configuration file contains:
      """
      [runtime]
      cleaner_interval = "7s"
      """
    When the runtime files are applied in order to a default broker
    Then the supplied Kafka configuration key origins are JSON []

  Scenario: Supplied coordinator fields record their Kafka keys
    Given a runtime configuration file contains:
      """
      [runtime]
      consumer_group_session_timeout = "45s"
      share_group_max_size = 200
      streams_group_rack_aware_assignment_tags = ["rack"]
      """
    When the runtime files are applied in order to a default broker
    Then the supplied Kafka configuration key origins are JSON ["group.consumer.session.timeout.ms","group.share.max.size","group.streams.rack.aware.assignment.tags"]

  Scenario: A second runtime overlay adds to existing origins
    Given a runtime configuration file contains:
      """
      [runtime]
      consumer_group_session_timeout = "45s"
      """
    Given a runtime configuration file contains:
      """
      [runtime]
      socket_send_buffer = "1MiB"
      """
    When the runtime files are applied in order to a default broker
    Then the supplied Kafka configuration key origins are JSON ["group.consumer.session.timeout.ms","socket.send.buffer.bytes"]
