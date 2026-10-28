Feature: DescribeClientQuotas validates entity filters before matching stored values
  Background:
    Given user alice has producer byte rate 1024 and the default user quota is 2048

  Scenario Outline: Invalid filter shapes fail the complete request before matching stored quotas
    Given the exact-type quota filter has components <components>
    When an admin describes quotas using that exact-type filter
    Then the complete quota description refusal has code <code> message <message> and null entries
    Examples:
      | components                                      | code | message                                                                                                                                |
      | empty:specified:null                            | 42   | Invalid empty entity type.                                                                                                             |
      | user:exact:alice;user:specified:null              | 42   | Entity type user cannot appear more than once in the filter.                                                                            |
      | group:specified:null                            | 35   | Unsupported entity type group                                                                                                          |
      | user:exact:null                                 | 42   | Request specified MATCH_TYPE_EXACT, but set match string to null.                                                                       |
      | user:default:alice                              | 42   | Request specified MATCH_TYPE_DEFAULT, but also specified a match string.                                                                |
      | user:specified:alice                            | 42   | Request specified MATCH_TYPE_SPECIFIED, but also specified a match string.                                                              |
      | user:7:null                                     | 42   | Unknown match type 7                                                                                                                   |
      | ip:specified:null;user:specified:null            | 42   | Invalid entity filter component combination. IP filter component should not be used with user or clientId filter component.             |
      | client-id:default:null;ip:exact:1.2.3.4          | 42   | Invalid entity filter component combination. IP filter component should not be used with user or clientId filter component.             |

  Scenario Outline: Exact and default filters preserve complete matching quota rows
    Given the exact-type quota filter has components <components>
    When an admin describes quotas using that exact-type filter
    Then the complete quota response names user <user> with producer byte rate <value>
    Examples:
      | components          | user    | value |
      | user:exact:alice    | alice   | 1024  |
      | user:default:null   | default | 2048  |

  Scenario: Specified-user matching includes default user entities
    Given the exact-type quota filter has components user:specified:null
    When an admin describes quotas using that exact-type filter
    Then the specified-user filter matches both default and named alice entities
