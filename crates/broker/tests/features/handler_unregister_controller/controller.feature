Feature: UnregisterBroker runs only against the active controller image
  Scenario Outline: Followers and observers refuse with the current controller hint
    Given node 1 receives unregistration while the active controller is <leader>
    When node 1 decides whether controller unregistration can run locally
    Then controller routing returns error <code> with message <message>
    Examples:
      | leader  | code | message                                          |
      | 1       | 0    | no refusal                                       |
      | 3       | 41   | The active controller appears to be node 3.       |
      | unknown | 41   | No controller appears to be active.               |
