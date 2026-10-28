Feature: src delegation token tests token visibility requires owner filter plus a relationship

  Scenario Outline: create matches kafka lifetime and saturation
    Then delegation token tests token visibility requires owner filter plus a relationship: create matches kafka lifetime and saturation for <example>

    Examples:
      | example                             |
      | negative request uses ceiling       |
      | later time adds deadlines           |
      | zero request uses ceiling           |
      | other negative request uses ceiling |
      | minimum request uses ceiling        |
      | positive request caps lifetime      |
      | oversized request uses ceiling      |
      | renewal period can extend expiry    |
      | maximum expiry saturates            |
      | both deadlines saturate             |
      | only expiry saturates               |
      | expiry and renewal saturate         |
      | zero ceiling is invalid             |
      | zero renewal is invalid             |
      | negative ceiling is invalid         |

  Scenario Outline: renew matches kafka period cap and expiry
    Then delegation token tests token visibility requires owner filter plus a relationship: renew matches kafka period cap and expiry for <example>

    Examples:
      | example                            |
      | requested shorter period           |
      | requested longer period            |
      | requested much longer period       |
      | negative period uses default       |
      | zero period uses default           |
      | other negative period uses default |
      | short period can shorten expiry    |
      | capped by maximum                  |
      | saturated expiry capped by maximum |
      | saturated expiry                   |
      | deadline equal to now is live      |
      | max equal to now caps renewal      |
      | expired deadline                   |
      | max before now is expired          |
      | zero default cannot renew expired  |
      | zero default is invalid while live |
