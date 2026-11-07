Feature: Operators prove the freeze registry from their own machine

  The check runs against public keys on the operator's machine, so the broker
  that served the registry is not the one that says it is authentic.

  Rule: A registry passes when the operator's keys prove every signed entry, and an unsigned entry is no failure

    Scenario Outline: A registry with nothing wrong in it verifies
      Given the registry holds <entries>
      And the operator's machine holds alice's public key
      When the operator verifies the registry
      Then the tool reports success

      Examples:
        | case                   | entries                                      |
        | one proved entry       | a freeze alice signed                        |
        | unsigned beside proved | a freeze alice signed and an unsigned freeze |
        | only unsigned          | an unsigned freeze                           |

  Rule: An entry whose key the machine does not hold could not be checked

    Scenario: A freeze signed with a key the operator does not hold is unverifiable
      Given the registry holds a freeze alice signed
      And the operator's machine holds only bob's public key
      When the operator verifies the registry
      Then the tool reports an unverifiable registry

  Rule: A signature that does not verify outranks a key that is not held

    Scenario: A freeze whose signature the local key does not verify fails
      Given the registry holds a freeze alice signed
      And the operator's machine holds another public key under alice's key id
      When the operator verifies the registry
      Then the tool reports a signature failure

    Scenario: A failed signature beside an unverifiable entry is still a failure
      Given the registry holds a freeze alice signed and a freeze carol signed
      And the operator's machine holds another public key under alice's key id
      When the operator verifies the registry
      Then the tool reports a signature failure

  Rule: A trust file that proves nothing stops the check

    Scenario: A trust file with no operator keys stops the verification
      Given the registry holds a freeze alice signed
      And the operator's machine holds a trust file with no operator keys
      When the operator verifies the registry
      Then the tool reports a signature failure
