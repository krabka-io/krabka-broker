@docker
Feature: Stock Kafka commands surface break glass policy and honor independent approvals
  Scenario: An unapproved stock topic deletion reports the gate and preserves the topic
    Given a healthy broker requires two person approval for privileged topic deletion with no approved proposals
    When the stock topic tool creates topic kfc9-doomed
    Then ordinary topic creation remains successful
    When the same stock tool requests deletion of that topic
    Then the deletion exits nonzero and prints these policy observations
      | observation |
      | org.apache.kafka.common.errors.PolicyViolationException |
      | break-glass refused delete_topic on kfc9-doomed: no approved proposal covers the request |
    When the operator lists topics after the rejected deletion
    Then topic kfc9-doomed remains visible in a successful listing

  Scenario: Two distinct approvals permit the unchanged stock unclean election request
    Given a gated SASL broker hosts topic kfc9-elect for proposer alice and distinct approvers bob and carol
    When alice invokes the stock unclean election command for kfc9-elect partition 0
    Then the unapproved election exits nonzero and prints these policy observations
      | observation |
      | org.apache.kafka.common.errors.PolicyViolationException |
      | break-glass refused unclean_elect_leaders on kfc9-elect-0: no approved proposal covers the request |
    When alice proposes that election and bob then carol approve it through their own authenticated sessions
    Then one distinct approver is insufficient and the second satisfies the required count
    When alice invokes the stock unclean election command for kfc9-elect partition 0
    Then the approved election exits zero and has these required and forbidden observations
      | observation | present |
      | org.apache.kafka.common.errors.PolicyViolationException | false |
      | break-glass refused | false |
      | kfc9-elect-0 | true |
