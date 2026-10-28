Feature: Broker heartbeat state follows Kafka fencing and controlled shutdown
  Shutdown waits for transferred leadership and replicated drain records unless
  the broker is already fenced or explicitly requests an immediate fenced stop.

  Scenario Outline: Decide the next broker state for <case>
    Given the broker control state is <current>
    And the heartbeat request and controller replication facts are JSON <facts>
    When the controller calculates the broker's next heartbeat state
    Then the next broker control state is <expected>

    Examples:
      | case | current | facts | expected |
      | fenced, caught up | Fenced | {"fence":false,"shutdown":false,"caught_up":true,"leaderships":false,"shutdown_offset":null,"lowest_active_offset":9223372036854775807} | Unfenced |
      | fenced, behind its registration | Fenced | {"fence":false,"shutdown":false,"caught_up":false,"leaderships":false,"shutdown_offset":null,"lowest_active_offset":9223372036854775807} | Fenced |
      | fenced, asking to stay fenced | Fenced | {"fence":true,"shutdown":false,"caught_up":true,"leaderships":false,"shutdown_offset":null,"lowest_active_offset":9223372036854775807} | Fenced |
      | fenced, asking to shut down | Fenced | {"fence":false,"shutdown":true,"caught_up":true,"leaderships":true,"shutdown_offset":null,"lowest_active_offset":9223372036854775807} | ShutdownNow |
      | unfenced | Unfenced | {"fence":false,"shutdown":false,"caught_up":true,"leaderships":false,"shutdown_offset":null,"lowest_active_offset":9223372036854775807} | Unfenced |
      | unfenced, asking to be fenced | Unfenced | {"fence":true,"shutdown":false,"caught_up":true,"leaderships":false,"shutdown_offset":null,"lowest_active_offset":9223372036854775807} | Fenced |
      | unfenced, asking to be fenced and to shut down | Unfenced | {"fence":true,"shutdown":true,"caught_up":true,"leaderships":true,"shutdown_offset":null,"lowest_active_offset":9223372036854775807} | ShutdownNow |
      | unfenced, asking to shut down with leaderships | Unfenced | {"fence":false,"shutdown":true,"caught_up":true,"leaderships":true,"shutdown_offset":null,"lowest_active_offset":9223372036854775807} | ControlledShutdown |
      | unfenced, asking to shut down with no leadership | Unfenced | {"fence":false,"shutdown":true,"caught_up":true,"leaderships":false,"shutdown_offset":null,"lowest_active_offset":0} | ShutdownNow |
      | in controlled shutdown with leaderships | ControlledShutdown | {"fence":false,"shutdown":false,"caught_up":true,"leaderships":true,"shutdown_offset":10,"lowest_active_offset":9223372036854775807} | ControlledShutdown |
      | in controlled shutdown, an active broker behind | ControlledShutdown | {"fence":false,"shutdown":false,"caught_up":true,"leaderships":false,"shutdown_offset":10,"lowest_active_offset":9} | ControlledShutdown |
      | in controlled shutdown, every active broker at the offset | ControlledShutdown | {"fence":false,"shutdown":false,"caught_up":true,"leaderships":false,"shutdown_offset":10,"lowest_active_offset":10} | ShutdownNow |
      | in controlled shutdown, no active broker | ControlledShutdown | {"fence":false,"shutdown":false,"caught_up":true,"leaderships":false,"shutdown_offset":10,"lowest_active_offset":9223372036854775807} | ShutdownNow |
