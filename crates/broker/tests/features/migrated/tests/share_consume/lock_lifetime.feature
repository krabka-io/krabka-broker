Feature: tests share consume lock lifetime

  Scenario: lock timeout redelivers
    Then share consume lock lifetime: lock timeout redelivers

  Scenario: delivery limit archives
    Then share consume lock lifetime: delivery limit archives

  Scenario: renew extends lock not redelivered
    Then share consume lock lifetime: renew extends lock not redelivered

  Scenario: no renew redelivers after lock expiry
    Then share consume lock lifetime: no renew redelivers after lock expiry
