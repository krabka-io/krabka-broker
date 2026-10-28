Feature: Operator proof for topic freezes and thaws
  Scenario Outline: require signature decides whether an unsigned freeze is accepted
    Given an operator broker with require_signature set to <require_signature>
    When a client probes writes to incident topic orders
    Then the incident topic write is accepted at log end 1
    When the operator sends an unsigned incident freeze for orders
    Then the operator mutation returns Kafka error <code>
    And the registry contains <entries> incident freeze entries
    When a client probes writes to incident topic orders
    Then the incident topic write is <outcome> at log end <offset>
    When a client writes to the signed-freeze control topic
    Then the signed-freeze control accepts its first record

    Examples:
      | require_signature | code | entries | outcome  | offset |
      | false             | 0    | 1       | refused  | 1      |
      | true              | 1010 | 0       | accepted | 2      |

  Scenario: a signed freeze round trips with its key id and signature intact
    Given an operator broker with require_signature set to false
    When the operator submits a freshly signed incident freeze for orders
    Then the operator mutation returns Kafka error 0
    And the incident registry entry preserves the signed fields and verifies locally
    When a client probes writes to incident topic orders
    Then the incident topic write is refused at log end 0
    When a client writes to the signed-freeze control topic
    Then the signed-freeze control accepts its first record

  Scenario Outline: an unsigned thaw is refused whatever require signature says
    Given an operator broker with require_signature set to <require_signature>
    And the operator has already signed an incident freeze for orders
    When an unsigned thaw attempts to lift the incident freeze
    Then the operator mutation returns Kafka error 1010
    And the incident registry still protects the orders scope
    When a client probes writes to incident topic orders
    Then the incident topic write is refused at log end 0
    When a client writes to the signed-freeze control topic
    Then the signed-freeze control accepts its first record

    Examples:
      | example            | require_signature |
      | default            | false             |
      | required signature | true              |

  Scenario: a signature captured from a freeze is refused as a thaw
    Given an operator broker with require_signature set to false
    And the operator has already signed an incident freeze for orders
    When an attacker replays the freeze signature as a thaw at original and fresh timestamps
    Then both replayed thaws return OPERATOR_SIGNATURE_INVALID
    And the incident registry still protects the orders scope
    When a client probes writes to incident topic orders
    Then the incident topic write is refused at log end 0
    When a client writes to the signed-freeze control topic
    Then the signed-freeze control accepts its first record

  Scenario: a signature survives a controller restart and still verifies
    Given a persistent operator broker has recorded a signed incident freeze
    When the signed-freeze controller restarts in rejoin mode
    Then the recovered entry and cluster identity are unchanged and the signature still verifies
    When a client probes writes to incident topic orders
    Then the incident topic write is refused at log end 0
    When a client writes to the signed-freeze control topic
    Then the signed-freeze control accepts its first record
