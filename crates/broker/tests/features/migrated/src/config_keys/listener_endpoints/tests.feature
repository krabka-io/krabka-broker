Feature: src config keys listener endpoints tests

  Scenario Outline: listeners read as kafka reads them
    Then config keys listener endpoints tests: listeners read as kafka reads them for <example>

    Examples:
      | example                    |
      | wildcard                   |
      | lower case name            |
      | bracketed ipv6             |
      | two                        |
      | unparsable                 |
      | port overflow              |
      | unmapped                   |
      | repeated name              |
      | repeated port              |
      | port zero may repeat       |
      | ipv4 and ipv6 share a port |
      | two ipv4 share a port      |
      | three share a port         |

  Scenario Outline: the protocol map reads as kafka reads it
    Then config keys listener endpoints tests: the protocol map reads as kafka reads it for <example>

    Examples:
      | example                    |
      | empty                      |
      | lower case                 |
      | later entry wins           |
      | no colon                   |
      | unknown protocol           |
      | names meet once normalised |
