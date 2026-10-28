Feature: IP quotas govern connection creation and release independently
  Scenario: An administrator changes and reads the per IP creation rate
    Given a SASL PLAIN quota administrator connects as admin
    When the administrator sets loopback connection creation rate to two per second
    Then the IP quota alteration succeeds
    When the administrator describes IP entities after the quota reaches the metadata image
    Then the sole described IP entity reports connection creation rate two

  Scenario: The canonical loopback IP rate limits connections independently
    Given IP entity 127.0.0.1 permits one new connection per second
    When two loopback connections request API versions while another loopback IP requests them independently
    Then the first connection is served the second closes without a response and the other IP is served within five hundred milliseconds

  Scenario Outline: Equivalent loopback address spellings share the same connection quota
    Given IP entity <entity> permits one new connection per second
    When two loopback connections request API versions while another loopback IP requests them independently
    Then the first connection is served the second closes without a response and the other IP is served within five hundred milliseconds
    Examples:
      | entity |
      | ::ffff:127.0.0.1 |
      | 127.1 |

  Scenario: Unconfigured connection creation rates leave the baseline unthrottled
    Given a plaintext broker has no connection creation rate quota
    When five TCP connections open without a configured rate limit
    Then the five connections open within five hundred milliseconds

  Scenario: A simultaneous connection cap releases capacity when the held connection closes
    Given a plaintext broker permits only one simultaneous connection per IP
    When a first connection requests API versions and remains open
    Then the connection inside the per IP cap is served
    When a second connection from the same IP requests API versions
    Then the excess connection is refused while the first holds its slot
    When the first connection closes and a fresh connection retries for at most five seconds
    Then a replacement connection is served after the original slot is released
