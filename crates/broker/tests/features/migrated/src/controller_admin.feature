Feature: Kafka controller-listener API advertisement
  The controller router advertises exactly the public controller surface of
  Kafka 4.3.1 and the Envelope version whose flexible header it can decode.

  Scenario: The controller router exposes Kafka's exact public controller surface
    Given a new broker controller-admin router
    When the controller router's supported API versions are queried
    Then the controller API keys occur once and are exactly:
      | key |
      | 19 |
      | 20 |
      | 29 |
      | 30 |
      | 31 |
      | 32 |
      | 33 |
      | 37 |
      | 38 |
      | 39 |
      | 40 |
      | 41 |
      | 43 |
      | 44 |
      | 45 |
      | 46 |
      | 49 |
      | 50 |
      | 51 |
      | 56 |
      | 57 |
      | 58 |
      | 62 |
      | 63 |
      | 64 |
      | 67 |
      | 73 |
      | 94 |

  Scenario: Envelope uses Kafka's version-zero flexible controller contract
    Given a new broker controller-admin router
    When the controller router's supported API versions are queried
    Then Envelope API 58 has minimum, maximum, released maximum, and flexible minimum all zero
