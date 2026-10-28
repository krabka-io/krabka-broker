Feature: Observer fetch corruption follows Kafka framing and loader policies
  Corrupt framing refuses a response; incomplete trailing batches preserve
  preceding whole batches; sound framing with bad records counts a load error.

  Scenario Outline: Apply a controller response containing <fault>
    Given a fetched topic sequence a, b, c has <fault> in the middle batch
    When the observer applies the fetched records from offset 0
    Then the observer resumes at <offset>, counts <errors> load errors, has timestamp <timestamp>, and reports no fatal fault
    And the published topics from that sequence are JSON <topics>

    Examples:
      | fault | offset | errors | timestamp | topics |
      | a corrupt CRC | 0 | 0 | unset | [] |
      | an unsupported magic byte | 0 | 0 | unset | [] |
      | a negative batch length | 0 | 0 | unset | [] |
      | a truncated body | 1 | 0 | 0 | ["a"] |
      | a truncated header | 1 | 0 | 0 | ["a"] |
      | unparseable records in sound framing | 3 | 1 | 0 | ["a"] |
