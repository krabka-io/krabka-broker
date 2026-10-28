Feature: Controller API release and header boundaries
  Request schemas determine API keys and flexible framing. Released bounds omit
  trunk-only APIs and unstable versions unless unstable API support is enabled.

  Scenario: Request-derived controller ranges retain release gates and framing (1: request ApiVersions, probe 5)
    Given controller request type ApiVersions
    When controller ranges and release gates are derived for probe version 5
    Then the controller API key is 18, minimum version is 0, and flexible framing starts at 3
    And released maximum is 4, unstable maximum is 5, and the probe is blocked true without unstable support and false with it

  Scenario: Request-derived controller ranges retain release gates and framing (2: request UnregisterController, probe 0)
    Given controller request type UnregisterController
    When controller ranges and release gates are derived for probe version 0
    Then the controller API key is 94, minimum version is 0, and flexible framing starts at 0
    And released maximum is unavailable, unstable maximum is 0, and the probe is blocked true without unstable support and false with it

  Scenario: Request-derived controller ranges retain release gates and framing (3: request SaslHandshake, probe 1)
    Given controller request type SaslHandshake
    When controller ranges and release gates are derived for probe version 1
    Then the controller API key is 17, minimum version is 0, and flexible framing starts at never
    And released maximum is 1, unstable maximum is 1, and the probe is blocked false without unstable support and false with it
