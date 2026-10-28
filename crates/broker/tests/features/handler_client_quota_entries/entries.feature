Feature: AlterClientQuotas validates values entity identity and duplicate entries like Kafka
  Background:
    Given user carol currently has producer byte rate 1024

  Scenario Outline: Quota keys enforce their numeric type and scope while removals skip value validation
    Given quota entries name entities <entities> with operations <operations>
    When the quota controller validates entries with IP literals and localhost resolvable
    Then the quota results name entities <entities> with outcome <outcome> and records <records>
    Examples:
      | entities    | operations                         | outcome                                                                      | records                                    |
      | user=alice  | set:producer_byte_rate=1024         | accepted                                                                     | user=alice/producer_byte_rate=1024          |
      | user=alice  | set:foo=1                          | Invalid configuration key foo                                                | none                                       |
      | user=alice  | remove:foo=0                       | accepted                                                                     | none                                       |
      | user=carol  | remove:producer_byte_rate=0         | accepted                                                                     | user=carol/producer_byte_rate=null          |
      | user=carol  | set:producer_byte_rate=1024         | accepted                                                                     | none                                       |
      | user=alice  | set:producer_byte_rate=0            | Quota producer_byte_rate must be greater than 0                               | none                                       |
      | user=alice  | set:request_percentage=-1           | Quota request_percentage must be greater than 0                              | none                                       |
      | user=alice  | set:producer_byte_rate=1024.5       | producer_byte_rate cannot be a fractional value.                             | none                                       |
      | user=alice  | set:consumer_byte_rate=inf          | Proposed value for consumer_byte_rate is too large for a LONG.                | none                                       |
      | ip=10.0.0.1 | set:connection_creation_rate=1.5    | connection_creation_rate cannot be a fractional value.                       | none                                       |
      | ip=10.0.0.1 | set:connection_creation_rate=3e9    | Proposed value for connection_creation_rate is too large for an INT.          | none                                       |
      | user=alice  | set:request_percentage=250          | accepted                                                                     | user=alice/request_percentage=250          |
      | user=alice  | set:controller_mutation_rate=inf    | accepted                                                                     | user=alice/controller_mutation_rate=inf    |
      | user=alice  | set:connection_creation_rate=1      | Invalid configuration key connection_creation_rate                           | none                                       |
      | ip=10.0.0.1 | set:producer_byte_rate=1            | Invalid configuration key producer_byte_rate                                 | none                                       |

  Scenario Outline: Entity rules preserve Kafka duplicate semantics and the records staged before a repeated entity
    Given quota entries name entities <entities> with operations <operations>
    When the quota controller validates entries with IP literals and localhost resolvable
    Then the quota results name entities <result_entities> with outcome <outcome> and records <records>
    Examples:
      | entities                    | operations                                                       | result_entities           | outcome                                                                                              | records                                                                           |
      | ip=10.0.0.1+user=alice       | set:connection_creation_rate=1                                   | ip=10.0.0.1+user=alice     | Invalid quota entity combination, IP entity shouldnot be combined with User or ClientId                | none                                                                              |
      | ip=::1;ip=localhost          | set:connection_creation_rate=5;set:connection_creation_rate=6     | ip=::1;ip=localhost        | accepted                                                                                             | ip=::1/connection_creation_rate=5;ip=localhost/connection_creation_rate=6            |
      | ip=not-an-ip                | set:connection_creation_rate=1                                   | ip=not-an-ip              | not-an-ip is not a valid IP or resolvable host.                                                        | none                                                                              |
      | user=empty                  | set:producer_byte_rate=1                                         | user=empty                | Empty user not supported                                                                             | none                                                                              |
      | group=g1                    | set:producer_byte_rate=1                                         | group=g1                  | Unhandled client quota entity type: group                                                             | none                                                                              |
      | none                        | set:producer_byte_rate=1                                         | none                      | Invalid empty client quota entity                                                                    | none                                                                              |
      | client-id=null+user=alice   | set:producer_byte_rate=1,set:producer_byte_rate=2                | client-id=null+user=alice | Ignoring duplicate entity ClientQuotaEntity(entries={client-id=null, user=alice})                      | none                                                                              |
      | user=alice;user=alice        | set:producer_byte_rate=1;set:consumer_byte_rate=2                 | user=alice                | Ignoring duplicate entity ClientQuotaEntity(entries={user=alice})                                      | user=alice/producer_byte_rate=1                                                   |
      | user=a+user=b               | set:producer_byte_rate=1                                         | user=b                    | accepted                                                                                             | user=b/producer_byte_rate=1                                                       |
