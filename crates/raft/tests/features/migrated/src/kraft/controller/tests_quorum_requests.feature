Feature: Inbound Kafka quorum request validation
  # Vote partitions: index, replica epoch/id, last offset epoch/offset, pre-vote.
  # Epoch partitions: index, leader id/epoch, then preferred candidates for End.
  # Snapshot partitions: index, leader epoch, snapshot end/epoch, position.
  # Followers start at node 1, voters 1/2/3, leader 2 epoch 5.
  # Snapshot leader is node 1 epoch 1 with 10/1 bytes 0123456789 and bootstrap 0/0.

  Scenario Outline: Vote validates the incoming request and returns the full Kafka response
    Given a Vote request with <request>
    When the quorum controller handles the Vote request
    Then the Vote response matches every expected Kafka field <expected>

    Examples:
      | case                                                   | request | expected |
      | a different cluster id                                 | {"cluster":"AAAAAAAAAAAAAAAAAAAABw","voter":1,"topics":[["__cluster_metadata",[[0,6,3,0,0,false]]]]} | {"code":104,"topics":[],"endpoints":[]} |
      | a malformed cluster id                                 | {"cluster":"not-a-cluster","voter":1,"topics":[["__cluster_metadata",[[0,6,3,0,0,false]]]]} | {"code":104,"topics":[],"endpoints":[]} |
      | a topic other than the metadata topic                  | {"cluster":null,"voter":1,"topics":[["other",[[0,6,3,0,0,false]]]]} | {"code":42,"topics":[],"endpoints":[]} |
      | a partition other than zero                            | {"cluster":null,"voter":1,"topics":[["__cluster_metadata",[[1,6,3,0,0,false]]]]} | {"code":42,"topics":[],"endpoints":[]} |
      | a repeated partition                                   | {"cluster":null,"voter":1,"topics":[["__cluster_metadata",[[0,6,3,0,0,false],[0,6,3,0,0,false]]]]} | {"code":42,"topics":[],"endpoints":[]} |
      | a repeated topic                                       | {"cluster":null,"voter":1,"topics":[["__cluster_metadata",[[0,6,3,0,0,false]]],["__cluster_metadata",[[0,6,3,0,0,false]]]]} | {"code":42,"topics":[],"endpoints":[]} |
      | a negative last offset                                 | {"cluster":null,"voter":1,"topics":[["__cluster_metadata",[[0,6,3,0,-1,false]]]]} | {"code":0,"topics":[["__cluster_metadata",[[0,42,2,5,false]]]],"endpoints":[[2,"127.0.0.1",9093]]} |
      | a negative last offset epoch                           | {"cluster":null,"voter":1,"topics":[["__cluster_metadata",[[0,6,3,-1,0,false]]]]} | {"code":0,"topics":[["__cluster_metadata",[[0,42,2,5,false]]]],"endpoints":[[2,"127.0.0.1",9093]]} |
      | a vote whose last epoch equals its candidate epoch     | {"cluster":null,"voter":1,"topics":[["__cluster_metadata",[[0,6,3,6,0,false]]]]} | {"code":0,"topics":[["__cluster_metadata",[[0,42,2,5,false]]]],"endpoints":[[2,"127.0.0.1",9093]]} |
      | a pre-vote whose last epoch passes its candidate epoch | {"cluster":null,"voter":1,"topics":[["__cluster_metadata",[[0,6,3,7,0,true]]]]} | {"code":0,"topics":[["__cluster_metadata",[[0,42,2,5,false]]]],"endpoints":[[2,"127.0.0.1",9093]]} |
      | a candidate epoch below the current epoch              | {"cluster":null,"voter":1,"topics":[["__cluster_metadata",[[0,4,3,0,0,true]]]]} | {"code":0,"topics":[["__cluster_metadata",[[0,74,2,5,false]]]],"endpoints":[[2,"127.0.0.1",9093]]} |
      | a negative candidate id                                | {"cluster":null,"voter":1,"topics":[["__cluster_metadata",[[0,6,-1,0,0,false]]]]} | {"code":0,"topics":[["__cluster_metadata",[[0,42,2,5,false]]]],"endpoints":[[2,"127.0.0.1",9093]]} |
      | a request addressed to another voter                   | {"cluster":null,"voter":2,"topics":[["__cluster_metadata",[[0,6,3,0,0,false]]]]} | {"code":0,"topics":[["__cluster_metadata",[[0,125,-1,6,false]]]],"endpoints":[]} |
      | a request addressed to an unknown voter id             | {"cluster":null,"voter":0,"topics":[["__cluster_metadata",[[0,6,3,0,0,false]]]]} | {"code":0,"topics":[["__cluster_metadata",[[0,125,-1,6,false]]]],"endpoints":[]} |
      | a pre-vote addressed to another voter                  | {"cluster":null,"voter":2,"topics":[["__cluster_metadata",[[0,5,3,0,0,true]]]]} | {"code":0,"topics":[["__cluster_metadata",[[0,125,2,5,false]]]],"endpoints":[[2,"127.0.0.1",9093]]} |
      | a pre-vote from a candidate outside the voters         | {"cluster":null,"voter":1,"topics":[["__cluster_metadata",[[0,5,0,0,0,true]]]]} | {"code":0,"topics":[["__cluster_metadata",[[0,0,2,5,false]]]],"endpoints":[[2,"127.0.0.1",9093]]} |
      | a pre-vote from a voter is granted                     | {"cluster":null,"voter":1,"topics":[["__cluster_metadata",[[0,5,3,0,0,true]]]]} | {"code":0,"topics":[["__cluster_metadata",[[0,0,2,5,true]]]],"endpoints":[[2,"127.0.0.1",9093]]} |
      | a pre-vote naming no voter is granted                  | {"cluster":null,"voter":-1,"topics":[["__cluster_metadata",[[0,5,3,0,0,true]]]]} | {"code":0,"topics":[["__cluster_metadata",[[0,0,2,5,true]]]],"endpoints":[[2,"127.0.0.1",9093]]} |
      | a vote in the current epoch of a known leader          | {"cluster":null,"voter":1,"topics":[["__cluster_metadata",[[0,5,3,0,0,false]]]]} | {"code":0,"topics":[["__cluster_metadata",[[0,0,2,5,false]]]],"endpoints":[[2,"127.0.0.1",9093]]} |

  Scenario Outline: BeginQuorumEpoch validates the incoming request and returns the full Kafka response
    Given a BeginQuorumEpoch request with <request>
    When the quorum controller handles the BeginQuorumEpoch request
    Then the BeginQuorumEpoch response matches every expected Kafka field <expected>

    Examples:
      | case                                      | request | expected |
      | a different cluster id                    | {"cluster":"AAAAAAAAAAAAAAAAAAAABw","topics":[["__cluster_metadata",[[0,3,6]]]],"voter":-1} | {"code":104,"topics":[],"endpoints":[]} |
      | a topic other than the metadata topic     | {"cluster":null,"topics":[["other",[[0,3,6]]]],"voter":-1} | {"code":42,"topics":[],"endpoints":[]} |
      | a repeated partition                      | {"cluster":null,"topics":[["__cluster_metadata",[[0,3,6],[0,3,6]]]],"voter":-1} | {"code":42,"topics":[],"endpoints":[]} |
      | an epoch below the current epoch          | {"cluster":null,"topics":[["__cluster_metadata",[[0,3,4]]]],"voter":-1} | {"code":0,"topics":[["__cluster_metadata",[[0,74,2,5]]]],"endpoints":[[2,"127.0.0.1",9093]]} |
      | a negative leader id                      | {"cluster":null,"topics":[["__cluster_metadata",[[0,-1,6]]]],"voter":-1} | {"code":0,"topics":[["__cluster_metadata",[[0,42,2,5]]]],"endpoints":[[2,"127.0.0.1",9093]]} |
      | a request addressed to another voter      | {"cluster":null,"topics":[["__cluster_metadata",[[0,3,6]]]],"voter":2} | {"code":0,"topics":[["__cluster_metadata",[[0,125,3,6]]]],"endpoints":[[3,"127.0.0.1",9093]]} |
      | a new leader in a newer epoch is followed | {"cluster":null,"topics":[["__cluster_metadata",[[0,3,6]]]],"voter":-1} | {"code":0,"topics":[["__cluster_metadata",[[0,0,3,6]]]],"endpoints":[[3,"127.0.0.1",9093]]} |

  Scenario Outline: EndQuorumEpoch validates the incoming request and returns the full Kafka response
    Given a EndQuorumEpoch request with <request>
    When the quorum controller handles the EndQuorumEpoch request
    Then the EndQuorumEpoch response matches every expected Kafka field <expected> and role <role>

    Examples:
      | case                                           | request | expected | role |
      | a different cluster id                         | {"cluster":"AAAAAAAAAAAAAAAAAAAABw","topics":[["__cluster_metadata",[[0,2,5,[]]]]]} | {"code":104,"topics":[],"endpoints":[]} | Follower |
      | a partition other than zero                    | {"cluster":null,"topics":[["__cluster_metadata",[[1,2,5,[]]]]]} | {"code":42,"topics":[],"endpoints":[]} | Follower |
      | an epoch below the current epoch               | {"cluster":null,"topics":[["__cluster_metadata",[[0,2,4,[]]]]]} | {"code":0,"topics":[["__cluster_metadata",[[0,74,2,5]]]],"endpoints":[[2,"127.0.0.1",9093]]} | Follower |
      | a negative leader id                           | {"cluster":null,"topics":[["__cluster_metadata",[[0,-1,5,[]]]]]} | {"code":0,"topics":[["__cluster_metadata",[[0,42,2,5]]]],"endpoints":[[2,"127.0.0.1",9093]]} | Follower |
      | this node first among the preferred successors | {"cluster":null,"topics":[["__cluster_metadata",[[0,2,5,[1,3]]]]]} | {"code":0,"topics":[["__cluster_metadata",[[0,0,-1,5]]]],"endpoints":[]} | Prospective |
      | no preferred successors                        | {"cluster":null,"topics":[["__cluster_metadata",[[0,2,5,[]]]]]} | {"code":0,"topics":[["__cluster_metadata",[[0,0,-1,5]]]],"endpoints":[]} | Prospective |
      | this node behind another preferred successor   | {"cluster":null,"topics":[["__cluster_metadata",[[0,2,5,[3,1]]]]]} | {"code":0,"topics":[["__cluster_metadata",[[0,0,-1,5]]]],"endpoints":[]} | Unattached |
      | this node absent from the preferred successors | {"cluster":null,"topics":[["__cluster_metadata",[[0,2,5,[3]]]]]} | {"code":0,"topics":[["__cluster_metadata",[[0,0,-1,5]]]],"endpoints":[]} | Unattached |

  Scenario Outline: FetchSnapshot validates the incoming request and returns the full Kafka response
    Given a FetchSnapshot request with <request>
    When the quorum controller handles the FetchSnapshot request
    Then the FetchSnapshot response matches every expected Kafka field <expected>

    Examples:
      | case                                                 | request | expected |
      | a different cluster id                               | {"cluster":"AAAAAAAAAAAAAAAAAAAABw","replica":2,"max_bytes":4,"topics":[["__cluster_metadata",[[0,1,10,1,0]]]]} | {"code":104,"topics":[],"endpoints":[]} |
      | a repeated partition                                 | {"cluster":null,"replica":2,"max_bytes":4,"topics":[["__cluster_metadata",[[0,1,10,1,0],[0,1,10,1,0]]]]} | {"code":42,"topics":[],"endpoints":[]} |
      | a topic other than the metadata topic                | {"cluster":null,"replica":2,"max_bytes":4,"topics":[["other",[[0,1,10,1,0]]]]} | {"code":0,"topics":[["other",[{"index":0,"code":3,"leader":null,"chunk":null}]]],"endpoints":[[1,"127.0.0.1",9093]]} |
      | a partition other than zero                          | {"cluster":null,"replica":2,"max_bytes":4,"topics":[["__cluster_metadata",[[1,1,10,1,0]]]]} | {"code":0,"topics":[["__cluster_metadata",[{"index":1,"code":3,"leader":null,"chunk":null}]]],"endpoints":[[1,"127.0.0.1",9093]]} |
      | a leader epoch below the current epoch               | {"cluster":null,"replica":2,"max_bytes":4,"topics":[["__cluster_metadata",[[0,0,10,1,0]]]]} | {"code":0,"topics":[["__cluster_metadata",[{"index":0,"code":74,"leader":[1,1],"chunk":null}]]],"endpoints":[[1,"127.0.0.1",9093]]} |
      | a leader epoch above the current epoch               | {"cluster":null,"replica":2,"max_bytes":4,"topics":[["__cluster_metadata",[[0,2,10,1,0]]]]} | {"code":0,"topics":[["__cluster_metadata",[{"index":0,"code":75,"leader":[1,1],"chunk":null}]]],"endpoints":[[1,"127.0.0.1",9093]]} |
      | a snapshot end offset with no snapshot               | {"cluster":null,"replica":2,"max_bytes":4,"topics":[["__cluster_metadata",[[0,1,11,1,0]]]]} | {"code":0,"topics":[["__cluster_metadata",[{"index":0,"code":98,"leader":[1,1],"chunk":null}]]],"endpoints":[[1,"127.0.0.1",9093]]} |
      | the bootstrap snapshot id                            | {"cluster":null,"replica":2,"max_bytes":4,"topics":[["__cluster_metadata",[[0,1,0,0,0]]]]} | {"code":0,"topics":[["__cluster_metadata",[{"index":0,"code":98,"leader":[1,1],"chunk":null}]]],"endpoints":[[1,"127.0.0.1",9093]]} |
      | a negative position                                  | {"cluster":null,"replica":2,"max_bytes":4,"topics":[["__cluster_metadata",[[0,1,10,1,-1]]]]} | {"code":0,"topics":[["__cluster_metadata",[{"index":0,"code":99,"leader":[1,1],"chunk":null}]]],"endpoints":[[1,"127.0.0.1",9093]]} |
      | a position at the snapshot size                      | {"cluster":null,"replica":2,"max_bytes":4,"topics":[["__cluster_metadata",[[0,1,10,1,10]]]]} | {"code":0,"topics":[["__cluster_metadata",[{"index":0,"code":99,"leader":[1,1],"chunk":null}]]],"endpoints":[[1,"127.0.0.1",9093]]} |
      | a position at the last byte                          | {"cluster":null,"replica":2,"max_bytes":4,"topics":[["__cluster_metadata",[[0,1,10,1,9]]]]} | {"code":0,"topics":[["__cluster_metadata",[{"index":0,"code":0,"leader":[1,1],"chunk":[10,1,10,9,"9"]}]]],"endpoints":[[1,"127.0.0.1",9093]]} |
      | a middle chunk capped by max bytes                   | {"cluster":null,"replica":2,"max_bytes":4,"topics":[["__cluster_metadata",[[0,1,10,1,4]]]]} | {"code":0,"topics":[["__cluster_metadata",[{"index":0,"code":0,"leader":[1,1],"chunk":[10,1,10,4,"4567"]}]]],"endpoints":[[1,"127.0.0.1",9093]]} |
      | the rest of the snapshot under the largest max bytes | {"cluster":null,"replica":2,"max_bytes":2147483647,"topics":[["__cluster_metadata",[[0,1,10,1,7]]]]} | {"code":0,"topics":[["__cluster_metadata",[{"index":0,"code":0,"leader":[1,1],"chunk":[10,1,10,7,"789"]}]]],"endpoints":[[1,"127.0.0.1",9093]]} |
      | a negative max bytes                                 | {"cluster":null,"replica":2,"max_bytes":-1,"topics":[["__cluster_metadata",[[0,1,10,1,0]]]]} | {"code":0,"topics":[["__cluster_metadata",[{"index":0,"code":0,"leader":[1,1],"chunk":[10,1,10,0,""]}]]],"endpoints":[[1,"127.0.0.1",9093]]} |
      | the first chunk capped by max bytes                  | {"cluster":null,"replica":2,"max_bytes":4,"topics":[["__cluster_metadata",[[0,1,10,1,0]]]]} | {"code":0,"topics":[["__cluster_metadata",[{"index":0,"code":0,"leader":[1,1],"chunk":[10,1,10,0,"0123"]}]]],"endpoints":[[1,"127.0.0.1",9093]]} |
