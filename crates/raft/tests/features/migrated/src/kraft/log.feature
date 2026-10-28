Feature: KRaft log leader-epoch end offsets
  Completed epochs end at the next epoch's first offset. Gaps resolve to the
  prior epoch, and requests newer than the current epoch resolve to the log end.

  Scenario Outline: On-disk epoch lookup follows Kafka boundary rules
    Given a metadata log with epoch 2 at offset 0 and epoch 4 at offset 1
    When the metadata log end offset for epoch <requested> is queried
    Then the metadata log answers epoch <found> ending at offset <offset>

    Examples:
      | boundary              | requested | found | offset |
      | older than the log    | 1         | 1     | 0      |
      | completed prior epoch | 2         | 2     | 1      |
      | gap between epochs    | 3         | 2     | 1      |
      | current epoch         | 4         | 4     | 2      |
      | newer than the log    | 9         | 4     | 2      |
