Feature: tests jvm acceptance sasl scram

  Scenario: jvm sasl scram sha512 produce consume
    Then jvm acceptance sasl scram: jvm sasl scram sha512 produce consume

  Scenario: jvm sasl scram sha256 produce consume
    Then jvm acceptance sasl scram: jvm sasl scram sha256 produce consume

  Scenario: jvm sasl scram sha512 in band reauth under max reauth window
    Then jvm acceptance sasl scram: jvm sasl scram sha512 in band reauth under max reauth window
