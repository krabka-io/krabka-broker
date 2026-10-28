Feature: Kafka control-key and end-transaction marker compatibility
  Scenario Outline: Nonnegative control-key versions preserve the type identifier
    Given control key bytes <key>
    When the control key type is parsed
    Then the control key type is <kind>

    Examples:
      | key          | kind |
      | 0000 0000    | 0    |
      | 0000 0001    | 1    |
      | 0000 03e8    | 1000 |
      | 0000 0001 0909 | 1  |
      | 0001 0001    | 1    |
      | 7fff 0001    | 1    |

  Scenario Outline: Negative control-key versions are corrupt
    Given control key bytes <key>
    When the control key type is parsed
    Then the control key rejects version <version>

    Examples:
      | key       | version |
      | ffff 0001 | -1      |
      | 8000 0001 | -32768  |

  Scenario Outline: Control keys require four bytes
    Given control key bytes <key>
    When the control key type is parsed
    Then the control key reports only <found> bytes

    Examples:
      | key     | found |
      | missing | 0     |
      | 00      | 1     |
      | 0000    | 2     |
      | 000000  | 3     |

  Scenario Outline: Nonnegative end-marker versions preserve the coordinator epoch
    Given end-marker value bytes <value>
    When the end-marker coordinator epoch is parsed
    Then the end-marker coordinator epoch is 7

    Examples:
      | value            |
      | 0000 00000007    |
      | 0000 00000007 09 |
      | 0001 00000007 09 |
      | 7fff 00000007    |

  Scenario Outline: Negative end-marker versions are corrupt
    Given end-marker value bytes <value>
    When the end-marker coordinator epoch is parsed
    Then the end-marker value rejects version <version>

    Examples:
      | value         | version |
      | ffff 00000007 | -1      |
      | 8000 00000007 | -32768  |

  Scenario Outline: End-marker values require six bytes
    Given end-marker value bytes <value>
    When the end-marker coordinator epoch is parsed
    Then the end-marker value reports only <found> bytes

    Examples:
      | value      | found |
      | missing    | 0     |
      | 0000       | 2     |
      | 00000000   | 4     |
      | 0000000000 | 5     |

  Scenario Outline: A malformed marker version cannot change the log
    Given producer 1000 epoch 2 has an open one-record transaction
    And control key bytes <key>
    And end-marker value bytes <value>
    When the malformed commit marker is appended
    Then the marker append refuses <record> version <version>
    And the log and complete transaction state stay unchanged at offset 1

    Examples:
      | key       | value         | record | version |
      | ffff 0001 | 0000 00000011 | key    | -1      |
      | 0000 0001 | fffe 00000011 | value  | -2      |

  Scenario Outline: A missing or short marker cannot change the log
    Given producer 1000 epoch 2 has an open one-record transaction
    And control key bytes <key>
    And end-marker value bytes <value>
    When the malformed commit marker is appended
    Then the marker append reports a <record> of only <found> bytes
    And the log and complete transaction state stay unchanged at offset 1

    Examples:
      | key       | value         | record | found |
      | 000000    | 0000 00000011 | key    | 3     |
      | 0000 0001 | missing       | value  | 0     |
