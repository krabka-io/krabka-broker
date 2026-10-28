Feature: LeaveGroup preserves missing-group and invalid-group wire semantics
  Scenario Outline: Missing and invalid groups use Kafka version-specific member errors
    Given a ready group coordinator with no group ghost
    And a LeaveGroup version <version> request for group <group> with identities <identities>
    When the group coordinator handles the LeaveGroup request
    Then LeaveGroup returns top-level error <code> with <members> and does not create the missing group
    Examples:
      | version | group | identities                                | code | members                                 |
      | 2       | ghost | legacy member m                           | 25   | no member rows                          |
      | 5       | ghost | member m-a and static instance inst-b      | 0    | unknown-member rows for both identities  |
      | 5       | empty | member m-a                                | 24   | no member rows                          |
      | 0       | empty | legacy member m                           | 24   | no member rows                          |
