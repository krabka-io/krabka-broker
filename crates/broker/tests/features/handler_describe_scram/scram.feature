Feature: DescribeUserScramCredentials reports each requested user's SCRAM credentials

  Rule: A user's credentials are listed by mechanism, SCRAM-SHA-256 first, in krabka's own order

    Scenario: Credentials are listed SCRAM-SHA-256 first whatever order they were stored in
      Given alice has a SCRAM-SHA-512 credential with 8192 iterations
      And alice has a SCRAM-SHA-256 credential with 4096 iterations
      When a client describes the SCRAM credentials with user list alice
      Then alice's credentials are listed as SCRAM-SHA-256 then SCRAM-SHA-512

  Rule: A request that names no user describes every user with a credential

    Scenario Outline: Every user is described, by name in krabka's own order, when the request names no user
      Given alice has a SCRAM-SHA-512 credential with 4096 iterations
      And bob has a SCRAM-SHA-512 credential with 8192 iterations
      When a client describes the SCRAM credentials with user list <users>
      Then the description lists alice with SCRAM-SHA-512 at 4096 iterations and bob with SCRAM-SHA-512 at 8192 iterations

      Examples:
        | case               | users |
        | a null user list   | null  |
        | an empty user list | empty |

  Rule: A request that names users describes only those users, and a user without a credential as RESOURCE_NOT_FOUND

    Scenario: A named user is described without the other users
      Given alice has a SCRAM-SHA-512 credential with 4096 iterations
      And bob has a SCRAM-SHA-512 credential with 8192 iterations
      When a client describes the SCRAM credentials with user list alice
      Then the description lists alice with SCRAM-SHA-512 at 4096 iterations

    Scenario: A named user without a credential is not found
      Given alice has a SCRAM-SHA-512 credential with 4096 iterations
      When a client describes the SCRAM credentials with user list ghost
      Then the description reports ghost as not found

  Rule: A user named twice is reported once as DUPLICATE_RESOURCE

    Scenario: A duplicated user collapses to one error row, in krabka's request order
      Given alice has a SCRAM-SHA-512 credential with 4096 iterations
      And bob has a SCRAM-SHA-512 credential with 8192 iterations
      When a client describes the SCRAM credentials with user list alice,bob,alice
      Then the description reports alice as a duplicate and lists bob with SCRAM-SHA-512 at 8192 iterations

  Rule: A denied request answers CLUSTER_AUTHORIZATION_FAILED with one anonymous row per requested name

    Scenario Outline: Authorization failure preserves request row count including duplicate users
      When a client without cluster Describe permission describes the SCRAM credentials with user list <users>
      Then the description fails with CLUSTER_AUTHORIZATION_FAILED in <rows> anonymous error rows

      Examples:
        | case                         | users           | rows |
        | a null user list             | null            | 0    |
        | an empty user list           | empty           | 0    |
        | a list with a duplicate name | alice,bob,alice | 3    |
