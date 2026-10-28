Feature: tests unit discovery

  Scenario: api versions round trip
    Then unit discovery: api versions round trip

  Scenario: metadata returns this broker and listed topics
    Then unit discovery: metadata returns this broker and listed topics

  Scenario: find coordinator creates the offsets topic then returns self
    Then unit discovery: find coordinator creates the offsets topic then returns self
