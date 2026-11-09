Feature: The ACL authorizer decides a request from the cluster's ACLs

  Rule: A request that no ACL allows is denied

    Scenario: A cluster without ACLs denies a request
      When User:alice requests to Read topic orders
      Then the request is denied

    Scenario Outline: An ACL that does not match the request does not allow it
      Given an ACL allows <principal> to Read topic <topic>
      When User:alice requests to Read topic orders
      Then the request is denied

      Examples:
        | case              | principal  | topic    |
        | another principal | User:bob   | orders   |
        | another topic     | User:alice | payments |

  Rule: A deny ACL wins over an allow ACL

    Scenario: A request that one ACL allows and another denies is denied
      Given an ACL allows User:alice to Read topic orders
      And an ACL denies User:alice to Read topic orders
      When User:alice requests to Read topic orders
      Then the request is denied

  Rule: allow.everyone.if.no.acl.found allows a request only on a resource that no ACL applies to

    Scenario: A cluster without ACLs allows a request
      Given the broker sets allow.everyone.if.no.acl.found to true
      When User:alice requests to Read topic orders
      Then the request is allowed

    Scenario Outline: An ACL that allows the request, or applies only elsewhere, allows it
      Given the broker sets allow.everyone.if.no.acl.found to true
      And an ACL allows User:alice to Read topic <topic>
      When User:alice requests to Read topic orders
      Then the request is allowed

      Examples:
        | case                           | topic    |
        | an ACL only on another topic   | payments |
        | an allow ACL for the principal | orders   |

    Scenario Outline: An ACL on the topic that does not allow the principal denies the request
      Given the broker sets allow.everyone.if.no.acl.found to true
      And an ACL <permission> <principal> to Read topic orders
      When User:alice requests to Read topic orders
      Then the request is denied

      Examples:
        | case                         | permission | principal  |
        | an ACL for another principal | allows     | User:bob   |
        | a deny ACL for the principal | denies     | User:alice |
