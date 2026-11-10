Feature: LeaveGroup preserves missing-group and invalid-group wire semantics

  Rule: Group Read authorization is checked before the group id

    Scenario: A caller denied group Read is refused even for an empty group id
      Given a group coordinator that denies every caller
      And a LeaveGroup version 5 request for group empty with identities member m-a
      When the group coordinator handles the LeaveGroup request
      Then LeaveGroup refuses the whole request with error 30

    Scenario: An authorized caller that names an empty group id is refused as an invalid group
      Given a group coordinator that allows every caller
      And a LeaveGroup version 5 request for group empty with identities member m-a
      When the group coordinator handles the LeaveGroup request
      Then LeaveGroup refuses the whole request with error 24

  Rule: A missing group answers with the shape of the request version

    Scenario Outline: A missing group uses Kafka version-specific member errors
      Given a ready group coordinator with no group ghost
      And a LeaveGroup version <version> request for group ghost with identities <identities>
      When the group coordinator handles the LeaveGroup request
      Then LeaveGroup returns top-level error <code> with <members> and does not create the missing group

      Examples:
        | case                              | version | identities                            | code | members                                 |
        | legacy leave of a missing group   | 2       | legacy member m                       | 25   | no member rows                          |
        | batched leave of a missing group  | 5       | member m-a and static instance inst-b | 0    | unknown-member rows for both identities |
