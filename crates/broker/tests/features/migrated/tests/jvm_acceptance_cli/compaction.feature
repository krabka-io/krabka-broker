@docker
Feature: Compacted topics retain the latest value of each key
  Scenario: Console consumers observe the latest values after compaction
    Given a compacted topic inherits 256 byte segments and a three second cleaner interval
    When the console producer appends repeated keys and enough padding to seal their segments
    Then all updates were acknowledged
    When two cleaner sweeps finish and the console consumer reads from the beginning
    Then only the latest value of every workload key remains and the cleaner reports a compaction
