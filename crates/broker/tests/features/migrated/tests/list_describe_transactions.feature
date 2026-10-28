Feature: Inspecting transaction coordinator state through Kafka administration APIs
  Scenario: A local transaction listing exposes ongoing work
    Given transactional id my-tid has an ongoing transaction on topic t-list
    When local transactions are listed without state filters
    Then the listing contains exactly the ongoing transactional id with its allocated producer id

  Scenario: A state filter excludes an ongoing transaction from Empty results
    Given transactional id my-tid has an ongoing transaction on topic t-state-filter
    When local transactions are listed with states Empty
    Then the successful listing has no matching transactions

  Scenario: Unknown state filters are distinguished from valid filters
    Given a broker has no initialized transactional ids
    When local transactions are listed with states Ongoing, BogusState, Empty
    Then only state filter BogusState is reported as unknown

  Scenario: Transaction details include its producer timing and registered partition
    Given transactional id describe-tid has an ongoing transaction on topic t-describe
    When details of the ongoing transaction are requested
    Then transaction details show describe-tid ongoing on t-describe partition 0 with timeout 60000

  Scenario: Describing an unknown transactional id returns its own not-found row
    Given a broker has no initialized transactional ids
    When an unknown transactional id ghost-tid is described
    Then transactional id ghost-tid is reported missing
