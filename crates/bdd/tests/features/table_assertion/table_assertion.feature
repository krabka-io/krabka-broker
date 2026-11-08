Feature: A step compares rows against its data table

  Scenario: A listing matches the table on every column
    Given these partitions exist
      | topic    | partition | leader epoch |
      | orders   | 0         | 5            |
      | payments | 0         | -            |
    When the partitions are listed
    Then the listing shows
      | topic    | partition | leader epoch |
      | orders   | 0         | 5            |
      | payments | 0         | -            |

  Scenario: A listing is compared only on the columns the table names
    Given these partitions exist
      | topic    | partition | leader epoch |
      | orders   | 0         | 5            |
      | payments | 0         | 7            |
    When the partitions are listed
    Then the listing shows
      | topic    | partition |
      | orders   | 0         |
      | payments | 0         |
