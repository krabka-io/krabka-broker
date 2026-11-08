Feature: Operators sign on their own machine

  Rule: The broker receives only the key id and the detached signature

    Scenario: A signed freeze reaches the registry as a key id and a signature
      When alice signs a freeze of the payments topic with alice's key
      Then the freeze on the payments topic names alice's key and carries a signature that alice's public key verifies
      And no part of alice's private key reached the broker

  Rule: A signature the tool cannot make stops before the broker

    Scenario: A key file that holds no signing key is a signature failure
      When alice tries to sign a freeze of the payments topic with a file that holds no key
      Then the tool reports a signature failure
      And the registry holds no freeze

  Rule: A signature the broker will not act on is a signature failure

    Scenario: A thaw that carries no signature is a signature failure
      Given alice has frozen the payments topic
      And alice holds a proposal to thaw literal:payments with 2 approvals
      When alice tries to thaw the payments topic with that proposal without signing
      Then the tool reports a signature failure

    Scenario: A freeze signed with another operator's key is a signature failure
      When bob tries to sign a freeze of the payments topic with alice's key
      Then the tool reports a signature failure
