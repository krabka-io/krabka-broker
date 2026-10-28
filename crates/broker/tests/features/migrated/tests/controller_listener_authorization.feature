Feature: tests controller listener authorization

  Scenario: a plaintext controller listener authorizes every request for anonymous
    Then controller listener authorization: a plaintext controller listener authorizes every request for anonymous

  Scenario: a sasl controller listener authorizes each request for its principal
    Then controller listener authorization: a sasl controller listener authorizes each request for its principal

  Scenario: an ssl controller listener authorizes each request for the certificate principal
    Then controller listener authorization: an ssl controller listener authorizes each request for the certificate principal
