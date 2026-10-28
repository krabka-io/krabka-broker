Feature: Bootstrap holds one coordinator group type per identifier
  Scenario Outline: Persisted record order controls group replacement and conflicting type failures
    Given the coordinator log contains group g records <records>
    When bootstrap replays the records under the single group identifier
    Then group g holds <kind> with load error <message>
    Examples:
      | records | kind | message |
      | offset | classic | none |
      | offset,consumer | consumer | none |
      | classic,consumer | failed | Group g is not a consumer group |
      | classic,classic-tombstone,consumer | consumer | none |
      | offset,share | failed | Group g is not a share group. |
      | consumer,share | failed | Group g is not a share group. |
      | consumer,classic | classic | none |
      | consumer,classic-tombstone | nothing | none |
