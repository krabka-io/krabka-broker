Feature: Each SCRAM credential alteration is validated on its own

  Rule: An alteration without cluster ALTER permission is refused

    Scenario: An upsertion without cluster ALTER permission is refused
      Given alice supplies a credential upsertion of mechanism type 1 with 4096 iterations
      When the controller validates the alteration without cluster ALTER permission
      Then alice's SCRAM alteration is refused with error 31 saying Request AlterUserScramCredentials needs ALTER permission.
      And no credential metadata record is staged

    Scenario: A deletion without cluster ALTER permission is refused before its missing credential is noticed
      Given alice asks to delete her mechanism type 1 credential, which does not exist
      When the controller validates the alteration without cluster ALTER permission
      Then alice's SCRAM alteration is refused with error 31 saying Request AlterUserScramCredentials needs ALTER permission.
      And no credential metadata record is staged

  Rule: An alteration names a known SCRAM mechanism

    Scenario: An upsertion of an unknown mechanism is refused
      Given alice supplies a credential upsertion of mechanism type 99 with 4096 iterations
      When the controller validates the alteration with cluster ALTER permission
      Then alice's SCRAM alteration is refused with error 33 saying Unknown SCRAM mechanism
      And no credential metadata record is staged

    Scenario: A deletion of an unknown mechanism is refused
      Given alice asks to delete her mechanism type 99 credential, which does not exist
      When the controller validates the alteration with cluster ALTER permission
      Then alice's SCRAM alteration is refused with error 33 saying Unknown SCRAM mechanism
      And no credential metadata record is staged

  Rule: An upsertion carries 4096 to 16384 iterations

    Scenario: An upsertion at the inclusive iteration ceiling is accepted
      Given alice supplies a credential upsertion of mechanism type 1 with 16384 iterations
      When the controller validates the alteration with cluster ALTER permission
      Then alice's SCRAM alteration is accepted with one staged credential record

    Scenario Outline: An upsertion outside the iteration range is refused
      Given alice supplies a credential upsertion of mechanism type 1 with <iterations> iterations
      When the controller validates the alteration with cluster ALTER permission
      Then alice's SCRAM alteration is refused with error 93 saying <message>
      And no credential metadata record is staged

      Examples:
        | case                            | iterations | message             |
        | one above the iteration ceiling | 16385      | Too many iterations |
        | one below the iteration floor   | 4095       | Too few iterations  |

  Rule: A deletion names an existing credential

    Scenario: A deletion of a missing credential is refused
      Given alice asks to delete her mechanism type 1 credential, which does not exist
      When the controller validates the alteration with cluster ALTER permission
      Then alice's SCRAM alteration is refused with error 91 saying Attempt to delete a user credential that does not exist
      And no credential metadata record is staged
