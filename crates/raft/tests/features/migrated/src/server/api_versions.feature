Feature: Controller ApiVersions release policy and finalized features
  @trunk
  Scenario Outline: Trunk APIs require the unstable API policy
    Given the controller router serves UnregisterController and unstable APIs are <enabled>
    When the controller advertises and checks UnregisterController version 0
    Then UnregisterController is advertised <enabled> and disabled <disabled>
    Examples:
      | enabled | disabled |
      | false | true |
      | true | false |
  @request
  Scenario Outline: Validate ApiVersions request bodies and release bounds
    Given an ApiVersions request with <request>
    When the controller handles the ApiVersions request under its release policy
    Then the ApiVersions answer consumes all bytes and matches its complete expected Kafka response <expected>

    Examples:
      | request | expected |
      | {"version":6,"unstable":false,"body":null} | {"version":0,"code":35,"apis":[[18,0,4]],"supported":[],"finalized":[],"epoch":-1} |
      | {"version":32767,"unstable":false,"body":null} | {"version":0,"code":35,"apis":[[18,0,4]],"supported":[],"finalized":[],"epoch":-1} |
      | {"version":-1,"unstable":false,"body":null} | {"version":0,"code":35,"apis":[[18,0,4]],"supported":[],"finalized":[],"epoch":-1} |
      | {"version":3,"unstable":false,"body":["","1.0",null,-1]} | {"version":3,"code":42,"apis":[],"supported":[],"finalized":[],"epoch":-1} |
      | {"version":4,"unstable":false,"body":["a b","1.0",null,-1]} | {"version":4,"code":42,"apis":[],"supported":[],"finalized":[],"epoch":-1} |
      | {"version":3,"unstable":false,"body":["krabka","",null,-1]} | {"version":3,"code":42,"apis":[],"supported":[],"finalized":[],"epoch":-1} |
      | {"version":5,"unstable":true,"body":["krabka","1.0","cluster",-1]} | {"version":5,"code":42,"apis":[],"supported":[],"finalized":[],"epoch":-1} |
      | {"version":5,"unstable":true,"body":["krabka","1.0",null,7]} | {"version":5,"code":42,"apis":[],"supported":[],"finalized":[],"epoch":-1} |
      | {"version":5,"unstable":true,"body":["krabka","1.0","other",8]} | {"version":5,"code":0,"apis":[[1,4,18],[17,0,1],[18,0,5],[36,0,2],[52,0,2],[53,0,1],[54,0,1],[55,0,2],[59,0,1],[60,0,2],[70,0,0],[80,0,1],[81,0,0],[82,0,0]],"supported":[["metadata.version",7,34],["group.version",0,1],["transaction.version",0,2],["share.version",0,2],["streams.version",0,1],["eligible.leader.replicas.version",0,1],["kraft.version",0,1],["krabka.version",0,2]],"finalized":[["metadata.version",7,7]],"epoch":-1} |
      | {"version":5,"unstable":true,"body":["krabka","1.0",null,-1]} | {"version":5,"code":0,"apis":[[1,4,18],[17,0,1],[18,0,5],[36,0,2],[52,0,2],[53,0,1],[54,0,1],[55,0,2],[59,0,1],[60,0,2],[70,0,0],[80,0,1],[81,0,0],[82,0,0]],"supported":[["metadata.version",7,34],["group.version",0,1],["transaction.version",0,2],["share.version",0,2],["streams.version",0,1],["eligible.leader.replicas.version",0,1],["kraft.version",0,1],["krabka.version",0,2]],"finalized":[["metadata.version",7,7]],"epoch":-1} |
      | {"version":0,"unstable":false,"body":["","",null,-1]} | {"version":0,"code":0,"apis":[[1,4,18],[17,0,1],[18,0,4],[36,0,2],[52,0,2],[53,0,1],[54,0,1],[55,0,2],[59,0,1],[60,0,2],[70,0,0],[80,0,1],[81,0,0],[82,0,0]],"supported":[],"finalized":[],"epoch":-1} |
      | {"version":6,"unstable":true,"body":null} | {"version":0,"code":35,"apis":[[18,0,5]],"supported":[],"finalized":[],"epoch":-1} |
      | {"version":5,"unstable":false,"body":["krabka","1.0",null,-1]} | {"version":0,"code":35,"apis":[[18,0,4]],"supported":[],"finalized":[],"epoch":-1} |
  @features
  Scenario Outline: Published feature levels reflect committed metadata
    Given a controller at kraft version <kraft> with finalized levels <input> and metadata epoch 1234
    When the controller answers feature fields at ApiVersions version <version>
    Then supported features use modern minimums <modern> and finalized levels are <output> at epoch 1234
    Examples:
      | kraft | input | version | modern | output |
      | 0 | metadata.version:25 | 3 | false | metadata.version:25 |
      | 0 | metadata.version:25,group.version:1 | 4 | true | group.version:1,metadata.version:25 |
      | 1 | metadata.version:25 | 4 | true | kraft.version:1,metadata.version:25 |
      | 1 | metadata.version:30,krabka.version:1 | 4 | true | krabka.version:1,kraft.version:1,metadata.version:30 |
  @features
  Scenario Outline: A fresh controller publishes Kafka initial finalized features
    Given a controller at kraft version <kraft> with finalized levels none and metadata epoch -1
    When the controller answers feature fields at ApiVersions version 4
    Then the fresh controller finalizes the minimum metadata version and kraft version <kraft> at epoch minus one
    Examples:
      | kraft |
      | 0 |
      | 1 |
