Feature: ShareFetch and ShareAcknowledge validate request identity before opening or continuing a session
  Scenario Outline: Null group authorization and member validity are checked in Kafka order
    Given only share group readable-group grants the consumer Read permission
    And share requests name group <group> and member <member>
    When the consumer sends ShareFetch at epoch 0 followed by ShareAcknowledge at epoch 1
    Then the complete ShareFetch response has error <fetch> and ShareAcknowledge has error <acknowledge>
    Examples:
      | group          | member        | fetch | acknowledge |
      | readable-group | 36 characters | 0     | 0           |
      | null           | 36 characters | 42    | 42          |
      | readable-group | null          | 42    | 42          |
      | readable-group | empty         | 42    | 42          |
      | readable-group | 37 characters | 42    | 42          |
      | other-group    | empty         | 30    | 30          |
