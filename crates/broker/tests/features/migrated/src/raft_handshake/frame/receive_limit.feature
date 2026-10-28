Feature: Controller SASL receive size is checked before payload allocation

  Scenario Outline: Check the inclusive SASL receive-size limit before payload reads
    Given the controller peer sent size prefix <size> and closed without a payload
    When the controller handshake admits a frame with SASL receive limit 4096
    Then frame admission reaches <outcome>

    Examples:
      | size | outcome |
      | 4096 | payload reading after an admitted prefix |
      | 4097 | SASL receive-size failure |
      | 4294967295 | SASL receive-size failure |
      | 2147483648 | SASL receive-size failure |
