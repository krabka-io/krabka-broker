Feature: An isolated controller never hosts audit replicas
  Scenario: only a broker holds the audit partition under an isolated controller
    Given a controller-only voter 1 and a joining broker-only node 2 with reserved listeners
    When the isolated controller starts and becomes quorum leader
    Then the controller alone has not created an audit topic
    When the broker-only node starts and joins the controller
    Then only broker 2 leads and replicates audit partition 0 and writes its startup record
