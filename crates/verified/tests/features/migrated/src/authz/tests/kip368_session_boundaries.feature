Feature: src authz tests kip368 session boundaries

  Scenario Outline: session expired for request matches kafka
    Then authz tests kip368 session boundaries: session expired for request matches kafka for <example>

    Examples:
      | example                                           |
      | no deadline never expires                         |
      | before the deadline                               |
      | at the deadline                                   |
      | one ms after the deadline                         |
      | api versions after the deadline                   |
      | sasl authenticate at the deadline                 |
      | sasl authenticate after the deadline              |
      | handshake that begins reauth                      |
      | handshake a second after the last reauth          |
      | handshake inside the reauth interval              |
      | handshake inside the interval before the deadline |
      | i64 max deadline never passes                     |
      | i64 min deadline                                  |
      | interval does not overflow                        |

  Scenario Outline: reauthentication begins matches kafka
    Then authz tests kip368 session boundaries: reauthentication begins matches kafka for <example>

    Examples:
      | example                         |
      | no expiry never reauthenticates |
      | first reauthentication          |
      | exactly a second later          |
      | one ms inside the interval      |
      | clock rollback                  |
      | wide gap saturates              |
      | wide rollback saturates         |
