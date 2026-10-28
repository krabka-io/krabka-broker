Feature: tests share consume acknowledgements

  Scenario: consume accept restart
    Then share consume acknowledgements: consume accept restart

  Scenario: release redelivers
    Then share consume acknowledgements: release redelivers

  Scenario: reject archives
    Then share consume acknowledgements: reject archives
