Feature: IncrementalAlterConfigs rejects malformed request shapes before authorization
  Scenario: Repeated resource identity is refused while a different resource type remains valid
    Given the incremental configuration request contains resources
      | type  | name | configs |
      | topic | t    | none    |
      | topic | t    | none    |
      | group | t    | none    |
    When the broker preprocesses duplicate resources duplicate keys and null operations
    Then the incremental request shape outcomes in order are
      | code | message                                 |
      | 42   | Each resource must appear at most once.  |
      | 42   | Each resource must appear at most once.  |
      | 0    | accepted                                |

  Scenario: Repeated configuration keys fail even when operations differ
    Given the incremental configuration request contains resources
      | type  | name | configs                                |
      | topic | t    | retention.ms:0:1,retention.ms:1:null     |
    When the broker preprocesses duplicate resources duplicate keys and null operations
    Then the incremental request shape outcomes in order are
      | code | message                            |
      | 42   | Error due to duplicate config keys |

  Scenario: Null values are permitted only for DELETE operations
    Given the incremental configuration request contains resources
      | type  | name | configs                    |
      | group | g    | a:0:null,b:1:null,c:2:null   |
    When the broker preprocesses duplicate resources duplicate keys and null operations
    Then the incremental request shape outcomes in order are
      | code | message                           |
      | 42   | Null value not supported for : a, c |
