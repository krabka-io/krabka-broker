Feature: tests max message bytes produce gate

  Scenario: a batch at the cap appends and one byte over it is refused
    Then max message bytes produce gate: a batch at the cap appends and one byte over it is refused

  Scenario: a recompressed batch is held to the cap at its stored size exactly
    Then max message bytes produce gate: a recompressed batch is held to the cap at its stored size exactly

  Scenario: a batch larger than segment bytes is refused even when the message cap allows it
    Then max message bytes produce gate: a batch larger than segment bytes is refused even when the message cap allows it

  Scenario: a topic with no override inherits the brokers message max bytes
    Then max message bytes produce gate: a topic with no override inherits the brokers message max bytes

  Scenario: a topic override binds below and above the broker default
    Then max message bytes produce gate: a topic override binds below and above the broker default

  Scenario: a batch the topic expands is measured after the broker re encodes it
    Then max message bytes produce gate: a batch the topic expands is measured after the broker re encodes it
