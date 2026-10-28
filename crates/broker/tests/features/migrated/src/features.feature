Feature: Metadata version transition compatibility
  A transition crosses a format change only when a changed version lies above
  the lower endpoint and at or below the higher endpoint, in either direction.

  Scenario Outline: Metadata transition boundaries match Kafka's exclusive lower endpoint
    Given metadata changes from version <source> to version <target>
    When Kafka's metadata-change boundaries are traversed
    Then a metadata format change is crossed <changed>

    Examples:
      | source | target | changed |
      | 25     | 25     | false   |
      | 25     | 24     | false   |
      | 24     | 25     | false   |
      | 29     | 24     | false   |
      | 25     | 22     | true    |
      | 25     | 23     | false   |
      | 23     | 22     | true    |
      | 17     | 15     | true    |
      | 16     | 15     | false   |
      | 13     | 12     | true    |
      | 12     | 11     | false   |
      | 30     | 29     | true    |
      | 32     | 31     | true    |
      | 31     | 30     | false   |
      | 8      | 7      | true    |
      | 7      | 6      | true    |
      | 33     | 32     | true    |
