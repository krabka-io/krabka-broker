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

  Rule: Each entry says what proves it, or why nothing does

    Scenario Outline: The operator reads what proves the freeze on the payments topic
      Given the registry holds <entries>
      And the operator's machine holds <keys>
      When the operator verifies the registry
      Then the report says the freeze on the payments topic is <proof>

      Examples:
        | case                  | entries               | keys                                    | proof                                                                           |
        | a local key proves it | a freeze alice signed | alice's public key                      | verified by alice-yubi                                                          |
        | nothing is signed     | an unsigned freeze    | alice's public key                      | unsigned, so the broker's word is all that stands behind it                     |
        | no local key          | a freeze alice signed | only bob's public key                   | no local operator key carries that key_id, so this entry cannot be checked here |
        | the wrong local key   | a freeze alice signed | another public key under alice's key id | the signature does not verify against the local operator key                    |

  Rule: The count names only the entries a signature proves

    Scenario Outline: The report ends with how many entries a signature proves
      Given the registry holds <entries>
      And the operator's machine holds alice's public key
      When the operator verifies the registry
      Then the report ends with <count> entries are proved by an operator signature

      Examples:
        | case                            | entries                                      | count  |
        | every entry is proved           | a freeze alice signed                        | 1 of 1 |
        | an unsigned entry is not proved | a freeze alice signed and an unsigned freeze | 1 of 2 |
        | nothing is proved               | an unsigned freeze                           | 0 of 1 |

  Rule: A registry that does not verify gives no count, and says why

    Scenario Outline: The operator reads why the registry did not verify
      Given the registry holds a freeze alice signed
      And the operator's machine holds <keys>
      When the operator verifies the registry
      Then the tool explains <explanation>
      And the report gives no count of proved entries

      Examples:
        | case                | keys                                    | explanation                                                            |
        | no local key        | only bob's public key                   | the registry names an operator key that this machine does not hold     |
        | the wrong local key | another public key under alice's key id | the registry does not verify against the operator keys on this machine |

  Rule: Only a check on the operator's machine claims a proof

    Scenario: A signed freeze listed without a check claims no proof
      Given the registry holds a freeze alice signed
      When the operator lists the registry without verifying it
      Then the report lists the freeze on the payments topic, set by alice, with no proof
