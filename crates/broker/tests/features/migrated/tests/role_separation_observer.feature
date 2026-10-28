Feature: tests role separation observer

  Scenario: broker only node observes and forwards
    Then role separation observer: broker only node observes and forwards

  Scenario: a broker only node recovers after the controller prunes past its fetch offset
    Then role separation observer: a broker only node recovers after the controller prunes past its fetch offset

  Scenario: heartbeats keep brokers unfenced in a role separated cluster
    Then role separation observer: heartbeats keep brokers unfenced in a role separated cluster

  Scenario: controller only node opens no client listener
    Then role separation observer: controller only node opens no client listener

  Scenario: broker only node forwards describe quorum to controller
    Then role separation observer: broker only node forwards describe quorum to controller

  Scenario: broker only nodes are described as quorum observers
    Then role separation observer: broker only nodes are described as quorum observers

  Scenario: unregister broker sent to a broker only node is decided by the controller
    Then role separation observer: unregister broker sent to a broker only node is decided by the controller

  Scenario: unregister broker sent to a broker only node reaches the controller
    Then role separation observer: unregister broker sent to a broker only node reaches the controller

  Scenario: a separate metadata log directory rolls cleans and restores a wiped node
    Then role separation observer: a separate metadata log directory rolls cleans and restores a wiped node
