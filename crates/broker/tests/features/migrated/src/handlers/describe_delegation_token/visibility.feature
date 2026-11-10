Feature: DescribeDelegationToken shows a caller only the tokens it may see

  Rule: A caller authenticated by a delegation token, or not authenticated, is refused before any other check

    Scenario: A caller authenticated by a delegation token cannot read any token
      Given alice owns tokens t-a,t-b
      And every operation is allowed by the authorizer
      When alice describes tokens on a connection authenticated by a delegation token
      Then the description is refused with DELEGATION_TOKEN_REQUEST_NOT_ALLOWED

    Scenario: The delegation token refusal precedes disabled token support
      Given the broker has no delegation token secret key
      When alice describes tokens on a connection authenticated by a delegation token
      Then the description is refused with DELEGATION_TOKEN_REQUEST_NOT_ALLOWED

    Scenario: An unauthenticated caller cannot read any token
      Given alice owns token t-a
      And every operation is allowed by the authorizer
      When an unauthenticated caller describes tokens
      Then the description is refused with DELEGATION_TOKEN_REQUEST_NOT_ALLOWED

  Rule: Without a token secret key the broker answers DELEGATION_TOKEN_AUTH_DISABLED

    Scenario: A broker without a token secret key describes no token
      Given the broker has no delegation token secret key
      When alice describes tokens owned by anyone
      Then the description is refused with DELEGATION_TOKEN_AUTH_DISABLED

  Rule: An empty owner list shows no token

    Scenario: An empty owner list hides even a token the caller owns
      Given alice owns token t-a
      And every operation is allowed by the authorizer
      When alice describes tokens with an empty owner list
      Then alice sees no token

  Rule: A caller sees only the tokens it owns, requested or may renew

    Scenario Outline: A caller related to a token sees it
      Given admin minted token t-minted for alice renewable by bob
      And carol owns token t-own
      When <caller> describes tokens owned by anyone
      Then <caller> sees token t-minted

      Examples:
        | case          | caller |
        | the requester | admin  |
        | the owner     | alice  |

    Scenario: A caller unrelated to a token sees no token
      Given admin minted token t-minted for alice renewable by bob
      When eve describes tokens owned by admin
      Then eve sees no token

  Rule: The owner filter matches a token by its owner, requester or renewer

    Scenario Outline: A filter naming a user related to the token finds it
      Given admin minted token t-minted for alice renewable by bob
      When <caller> describes tokens owned by <owners>
      Then <caller> sees token t-minted

      Examples:
        | case                           | caller | owners |
        | the requester naming itself    | admin  | admin  |
        | the requester naming the owner | admin  | alice  |
        | a renewer naming the requester | bob    | admin  |

    Scenario: A filter naming an unrelated user hides the token
      Given admin minted token t-minted for alice renewable by bob
      And carol owns token t-own
      When admin describes tokens owned by carol
      Then admin sees no token

    Scenario: An owner filter naming several users matches a token by any of them
      Given bob owns token t-b renewable by carol
      And dave owns token t-d
      When carol describes tokens owned by bob,dave
      Then carol sees token t-b

  Rule: A Describe ACL on a delegation token grants exactly that token

    Scenario: A Describe ACL on one token does not reveal the owner's other token
      Given alice owns tokens t-a,t-b
      And bob may describe delegation token t-a
      When bob describes tokens owned by anyone
      Then bob sees token t-a

  Rule: DescribeTokens on User:<owner> grants every token of that owner

    Scenario Outline: A DescribeTokens grant on the owner shows bob all of the owner's tokens
      Given alice owns tokens t-a1,t-a2
      And carol owns token t-c
      And user alice carries the ACLs <acls>
      When bob describes tokens owned by anyone
      Then bob sees tokens t-a1,t-a2

      Examples:
        | case                                          | acls                                            |
        | DescribeTokens grants all of the owner tokens | bob allow DescribeTokens                        |
        | a deny on another operation does not block it | bob deny CreateTokens; bob allow DescribeTokens |

    Scenario Outline: Without a DescribeTokens grant on the owner bob sees none of the owner's tokens
      Given alice owns tokens t-a1,t-a2
      And carol owns token t-c
      And user alice carries the ACLs <acls>
      When bob describes tokens owned by anyone
      Then bob sees no token

      Examples:
        | case                                         | acls                                   |
        | CreateTokens grants no description           | bob allow CreateTokens                 |
        | a deny on DescribeTokens wins over allow All | bob allow All; bob deny DescribeTokens |

    Scenario: An ACL for another principal on the owner suppresses the allow-by-default authorizer
      Given these tokens exist
        | token | owner |
        | t-a1  | alice |
        | t-c   | carol |
      And the authorizer allows a resource without ACLs but no principal may describe a token by its id
      And user alice carries the ACLs mallory allow DescribeTokens
      When bob describes tokens owned by anyone
      Then bob sees token t-c
