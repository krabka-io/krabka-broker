Feature: tests process kill durability

  Scenario: flush on append stays off by default
    Then process kill durability: flush on append stays off by default

  Scenario: torn tail is a partial trailing batch
    Then process kill durability: torn tail is a partial trailing batch

  Scenario: acks all survives a sigkill of the broker process
    Then process kill durability: acks all survives a sigkill of the broker process
