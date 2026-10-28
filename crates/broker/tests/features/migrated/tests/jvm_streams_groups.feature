@docker
Feature: Stock Streams administration discovers and describes a live native Streams group
  Scenario: A stock Streams administrator negotiates lists and describes the full live group
    Given a broker finalizes streams version one and hosts topic streams-input with 2 partitions
    When a sole native Streams member joins group jvm-streams-g and converges on both input partitions
    Then the join succeeds with a broker minted member identity and 2 active partitions
    When the member heartbeats and the stock Streams group tool describes the live group with wire logging
    Then the stock admin wire log has these required and forbidden protocol observations
      | observation | present |
      | Received API_VERSIONS response | true |
      | apiKey=89 | true |
      | FinalizedFeatureKey(name='streams.version' | true |
      | Sending LIST_GROUPS request | true |
      | typesFilter=[Streams] | true |
      | Received LIST_GROUPS response | true |
      | errorCode=0 | true |
      | Received STREAMS_GROUP_DESCRIBE response | true |
      | missing the topology information | false |
      | subtopologyId='0' | true |
      | groupId='jvm-streams-g' | true |
