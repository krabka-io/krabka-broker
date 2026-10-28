Feature: Every privileged Kafka transition enforces the two-person rule

  Scenario Outline: every gated transition refuses without a proposal and runs with one
    Given a two-person-rule broker with topic <topic> and Kafka transition action <action>
    When alice invokes the Kafka transition
    Then the transition returns error 44 and its refusal counter increases by 1
    When alice proposes the transition target and bob and carol approve
    And alice invokes the Kafka transition
    Then the transition returns error <approved> and the proposal consumed state is <consumed>

    Examples:
      | topic   | action | approved | consumed |
      | gated-0 | 2      | 84       | false    |
      | gated-1 | 5      | 85       | false    |
      | gated-2 | 7      | 0        | true     |
      | gated-3 | 6      | 0        | true     |
      | gated-4 | 4      | 0        | true     |
