Feature: Kafka control-key and end-transaction marker compatibility
  Scenario Outline: Nonnegative control-key versions preserve the type identifier
    Given control key bytes <key>
    When the control key type is parsed
    Then the control key type is <kind>

    Examples:
      | case                              | key            | kind |
      | an abort marker at version zero   | 0000 0000      | 0    |
      | a commit marker at version zero   | 0000 0001      | 1    |
      | an unknown type at version zero   | 0000 03e8      | 1000 |
      | trailing bytes after the type     | 0000 0001 0909 | 1    |
      | a newer version                   | 0001 0001      | 1    |
      | the highest version               | 7fff 0001      | 1    |

  Scenario Outline: Negative control-key versions are corrupt
    Given control key bytes <key>
    When the control key type is parsed
    Then the control key rejects version <version>

    Examples:
      | case                  | key       | version |
      | version minus one     | ffff 0001 | -1      |
      | the lowest version    | 8000 0001 | -32768  |

  Scenario Outline: Control keys require four bytes
    Given control key bytes <key>
    When the control key type is parsed
    Then the control key reports only <found> bytes

    Examples:
      | case                        | key     | found |
      | no key                      | missing | 0     |
      | one byte                    | 00      | 1     |
      | a version without a type    | 0000    | 2     |
      | one byte short              | 000000  | 3     |

  Scenario Outline: Nonnegative end-marker versions preserve the coordinator epoch
    Given end-marker value bytes <value>
    When the end-marker coordinator epoch is parsed
    Then the end-marker coordinator epoch is 7

    Examples:
      | case                               | value            |
      | version zero                       | 0000 00000007    |
      | trailing bytes at version zero     | 0000 00000007 09 |
      | trailing bytes at a newer version  | 0001 00000007 09 |
      | the highest version                | 7fff 00000007    |

  Scenario Outline: Negative end-marker versions are corrupt
    Given end-marker value bytes <value>
    When the end-marker coordinator epoch is parsed
    Then the end-marker value rejects version <version>

    Examples:
      | case                  | value         | version |
      | version minus one     | ffff 00000007 | -1      |
      | the lowest version    | 8000 00000007 | -32768  |

  Scenario Outline: End-marker values require six bytes
    Given end-marker value bytes <value>
    When the end-marker coordinator epoch is parsed
    Then the end-marker value reports only <found> bytes

    Examples:
      | case                        | value      | found |
      | no value                    | missing    | 0     |
      | a version without an epoch  | 0000       | 2     |
      | a partial epoch             | 00000000   | 4     |
      | one byte short              | 0000000000 | 5     |

  Scenario Outline: A malformed marker version cannot change the log
    Given producer 1000 epoch 2 has an open one-record transaction
    When a commit marker with control key bytes <key> and end-marker value bytes <value> is appended
    Then the marker append refuses <record> version <version>
    And the log and complete transaction state stay unchanged at offset 1

    Examples:
      | case                     | key       | value         | record | version |
      | a negative key version   | ffff 0001 | 0000 00000011 | key    | -1      |
      | a negative value version | 0000 0001 | fffe 00000011 | value  | -2      |

  Scenario Outline: A missing or short marker cannot change the log
    Given producer 1000 epoch 2 has an open one-record transaction
    When a commit marker with control key bytes <key> and end-marker value bytes <value> is appended
    Then the marker append reports a <record> of only <found> bytes
    And the log and complete transaction state stay unchanged at offset 1

    Examples:
      | case            | key       | value         | record | found |
      | a short key     | 000000    | 0000 00000011 | key    | 3     |
      | a missing value | 0000 0001 | missing       | value  | 0     |
