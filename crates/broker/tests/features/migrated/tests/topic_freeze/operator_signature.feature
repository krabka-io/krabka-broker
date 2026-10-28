Feature: tests topic freeze operator signature

  Scenario Outline: require signature decides whether an unsigned freeze is accepted
    Then topic freeze operator signature: require signature decides whether an unsigned freeze is accepted for <example>

    Examples:
      | example            |
      | attestation        |
      | required signature |

  Scenario: a signed freeze round trips with its key id and signature intact
    Then topic freeze operator signature: a signed freeze round trips with its key id and signature intact

  Scenario Outline: an unsigned thaw is refused whatever require signature says
    Then topic freeze operator signature: an unsigned thaw is refused whatever require signature says for <example>

    Examples:
      | example            |
      | default            |
      | required signature |

  Scenario: a signature captured from a freeze is refused as a thaw
    Then topic freeze operator signature: a signature captured from a freeze is refused as a thaw

  Scenario: a signature survives a controller restart and still verifies
    Then topic freeze operator signature: a signature survives a controller restart and still verifies
