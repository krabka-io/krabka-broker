Feature: Unclean leader elections recover partitions without a live ISR

  Scenario: Unclean election selects the first live replica when the ISR is unavailable
    Given three plaintext brokers are registered for operator leader elections
    And foo-unclean partition 0 has live replicas 1 and 2 but its committed leader and sole ISR member are unregistered broker 99
    When the operator requests an unclean election of foo-unclean partition 0
    Then ElectLeaders returns partition 0 with success code 0
    And foo-unclean partition 0 is led by live replica 1 and its ISR includes broker 1
