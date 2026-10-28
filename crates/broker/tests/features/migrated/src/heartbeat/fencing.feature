Feature: Broker registration transitions write and replay Kafka change records
  Fencing and controlled shutdown update only their own flags at the registered epoch.

  Scenario: Registration transition fence from fenced false and shutdown false
    Given broker 1 is registered with fenced false and controlled shutdown false
    When the controller requests fence and replays any resulting registration change
    Then the registration change has fencing Fence and controlled shutdown flag false
    And broker 1 retains its identity with fenced true and controlled shutdown false

  Scenario: Registration transition unfence from fenced true and shutdown false
    Given broker 1 is registered with fenced true and controlled shutdown false
    When the controller requests unfence and replays any resulting registration change
    Then the registration change has fencing Unfence and controlled shutdown flag false
    And broker 1 retains its identity with fenced false and controlled shutdown false

  Scenario: Registration transition controlled shutdown from fenced false and shutdown false
    Given broker 1 is registered with fenced false and controlled shutdown false
    When the controller requests controlled shutdown and replays any resulting registration change
    Then the registration change has fencing None and controlled shutdown flag true
    And broker 1 retains its identity with fenced false and controlled shutdown true

  Scenario: Registration transition fence from fenced false and shutdown true
    Given broker 1 is registered with fenced false and controlled shutdown true
    When the controller requests fence and replays any resulting registration change
    Then the registration change has fencing Fence and controlled shutdown flag false
    And broker 1 retains its identity with fenced true and controlled shutdown true

  Scenario Outline: An already satisfied transition emits no redundant record
    Given broker 1 is registered with fenced <fenced> and controlled shutdown <shutdown>
    When the controller requests <operation> and replays any resulting registration change
    Then the controller writes no redundant registration change
    And broker 1 retains its identity with fenced <fenced> and controlled shutdown <shutdown>

    Examples:
      | fenced | shutdown | operation |
      | true | false | fence |
      | false | false | unfence |
      | false | true | controlled shutdown |
