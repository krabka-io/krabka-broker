Feature: OffsetDelete combines broker checks and coordinator subscription decisions
  Scenario Outline: Authorization existence subscription and merge ordering determine complete rows and tombstones
    Given offset deletion requests topic partitions <requests>
    And the group subscribes to <subscription>
    When offset deletion validates topics t1 with 4 partitions t2 with 8 denied with 4 and absent ghost
    Then the ordered offset deletion rows are <rows>
    And the only offset keys eligible for tombstones are <deletes>
    Examples:
      | requests        | subscription | rows                 | deletes          |
      | t1:0,2;t2:7     | no topics    | t1:0=0,2=0;t2:7=0   | t1:0;t1:2;t2:7   |
      | denied:0,1      | no topics    | denied:0=29,1=29    | none             |
      | denied:0        | denied       | denied:0=29         | none             |
      | ghost:0         | ghost        | ghost:0=3           | none             |
      | t1:0            | t1           | t1:0=86             | none             |
      | t1:0;t2:1       | all topics   | t1:0=86;t2:1=86     | none             |
      | t1:0,99,-1,3    | t2           | t1:99=3,-1=3,0=0,3=0| t1:0;t1:3        |
      | t1:0;denied:0   | no topics    | denied:0=29;t1:0=0  | t1:0             |
