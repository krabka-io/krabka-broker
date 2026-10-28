Feature: tests unit producer id

  Scenario: init producer id returns fresh pid
    Then unit producer id: init producer id returns fresh pid

  Scenario: init producer id without coordinator bootstrap returns not coordinator
    Then unit producer id: init producer id without coordinator bootstrap returns not coordinator

  Scenario: find coordinator txn creates topic then returns local broker
    Then unit producer id: find coordinator txn creates topic then returns local broker

  Scenario: init producer id with transactional id returns real pid
    Then unit producer id: init producer id with transactional id returns real pid

  Scenario: init producer id with same tid bumps epoch
    Then unit producer id: init producer id with same tid bumps epoch
