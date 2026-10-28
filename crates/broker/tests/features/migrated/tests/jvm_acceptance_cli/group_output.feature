Feature: Reading consumer-group administration tool results
  Scenario: Reset table headers and padding do not become offset rows
    Given the consumer group tool printed
      """

      GROUP                          TOPIC                          PARTITION  NEW-OFFSET
      krabka-grp                     krabka-topic                   0          7
      krabka-grp                     krabka-topic                   1          0
      """
    When reset offsets are read from the table
    Then every reset table contains exactly these sorted offsets
      | group      | topic        | partition | new_offset |
      | krabka-grp | krabka-topic | 0         | 7          |
      | krabka-grp | krabka-topic | 1         | 0          |

  Scenario: Reset table order does not change its normalized offsets
    Given the consumer group tool printed
      """

      GROUP                          TOPIC                          PARTITION  NEW-OFFSET
      krabka-grp                     krabka-topic                   0          7
      krabka-grp                     krabka-topic                   1          0
      """
    When reset offsets are read with the table lines in either order
    Then every reset table contains exactly these sorted offsets
      | group      | topic        | partition | new_offset |
      | krabka-grp | krabka-topic | 0         | 7          |
      | krabka-grp | krabka-topic | 1         | 0          |

  Scenario: A reset table header contains no offset rows
    Given the consumer group tool printed
      """
      GROUP TOPIC PARTITION NEW-OFFSET
      """
    When reset offsets are read from the table
    Then every reset table contains exactly these sorted offsets
      | group | topic | partition | new_offset |

  Scenario: An inactive-group warning contains no offset rows
    Given the consumer group tool printed
      """
      Error: Assignments can only be reset if the group is inactive
      """
    When reset offsets are read from the table
    Then every reset table contains exactly these sorted offsets
      | group | topic | partition | new_offset |

  Scenario: A tool stack frame contains no offset rows
    Given the consumer group tool printed
      """
      at org.apache.kafka.tools.ConsumerGroupCommand.main(x.java:1)
      """
    When reset offsets are read from the table
    Then every reset table contains exactly these sorted offsets
      | group | topic | partition | new_offset |

  Scenario: Empty stdout contains no offset rows
    Given the consumer group tool printed
      """

      """
    When reset offsets are read from the table
    Then every reset table contains exactly these sorted offsets
      | group | topic | partition | new_offset |

  Scenario: Single-group export CSV omits the group column
    Given the consumer group tool printed
      """
      t,0,12
      """
    When exported offsets are read from CSV
    Then every exported table contains exactly
      | group  | topic | partition | offset |
      | absent | t     | 0         | 12     |

  Scenario: All-groups export CSV retains the group column
    Given the consumer group tool printed
      """
      g,t,0,12
      """
    When exported offsets are read from CSV
    Then every exported table contains exactly
      | group | topic | partition | offset |
      | g     | t     | 0         | 12     |

  Scenario: Blank exported CSV contains no offsets
    Given the exported CSV consists of two blank lines
    When exported offsets are read from CSV
    Then every exported table contains exactly
      | group | topic | partition | offset |

  Scenario: Quoting a single-group CSV field preserves the exported offset
    Given the consumer group tool printed
      """
      "t",0,12
      """
    When the quoted and unquoted forms of the exported CSV are read
    Then every exported table contains exactly
      | group  | topic | partition | offset |
      | absent | t     | 0         | 12     |

  Scenario: Quoting all-groups CSV fields preserves the exported offset
    Given the consumer group tool printed
      """
      "g","t",0,12
      """
    When the quoted and unquoted forms of the exported CSV are read
    Then every exported table contains exactly
      | group | topic | partition | offset |
      | g     | t     | 0         | 12     |

  Scenario: A doubled quote inside a CSV field becomes one literal quote
    Given the consumer group tool printed
      """
      "a""b",0,12
      """
    When exported offsets are read from CSV
    Then every exported table contains exactly
      | group  | topic | partition | offset |
      | absent | a"b   | 0         | 12     |

  Scenario: Repeated exception classes are reported only once
    Given the consumer group tool printed
      """
      Error: org.apache.kafka.common.errors.GroupNotEmptyException: no
      caused by org.apache.kafka.common.errors.GroupNotEmptyException
      and org.apache.kafka.common.errors.GroupIdNotFoundException: gone
      """
    When Kafka exception classes are read from the output
    Then the unique Kafka exception classes are exactly
      | class                                                     |
      | org.apache.kafka.common.errors.GroupIdNotFoundException    |
      | org.apache.kafka.common.errors.GroupNotEmptyException      |

  Scenario: Ordinary prose contains no Kafka exception classes
    Given the consumer group tool printed
      """
      nothing here
      """
    When Kafka exception classes are read from the output
    Then the unique Kafka exception classes are exactly
      | class |

  Scenario: Refused deletions retain a distinct failure for each group
    Given the consumer group tool printed
      """
      Error: Deletion of some consumer groups failed:
      * Group 'ghost' could not be deleted due to: org.apache.kafka.common.errors.GroupIdNotFoundException: The group id does not exist.
      * Group 'busy' could not be deleted due to: org.apache.kafka.common.errors.GroupNotEmptyException: The group is not empty.
      """
    When consumer group deletion outcomes are read
    Then the sorted group deletion outcomes are exactly
      | group | failure                                                   |
      | busy  | org.apache.kafka.common.errors.GroupNotEmptyException      |
      | ghost | org.apache.kafka.common.errors.GroupIdNotFoundException    |

  Scenario: A successful deletion reports its group without an exception
    Given the consumer group tool printed
      """
      Deletion of requested consumer groups ('gone') was successful.
      """
    When consumer group deletion outcomes are read
    Then the sorted group deletion outcomes are exactly
      | group | failure |
      | gone  | success |
