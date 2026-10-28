Feature: tests dynamic producer and group config

  Scenario: a dynamic producer id expiration reaches the running expiry sweep
    Then dynamic producer and group config: a dynamic producer id expiration reaches the running expiry sweep

  Scenario: a dynamic consumer assignment interval holds back the next assignment
    Then dynamic producer and group config: a dynamic consumer assignment interval holds back the next assignment

  Scenario: an assignment interval above the broker maximum is refused
    Then dynamic producer and group config: an assignment interval above the broker maximum is refused
