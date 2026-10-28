Feature: Offset commit value version follows per-commit expiry
  Scenario Outline: Expiring commits use legacy version one and ordinary commits use flexible version four
    Given offset 42 at leader epoch 4 has metadata meta commit time 1000000 expiry <expiry> and topic identifier <input_topic>
    When the coordinator serializes the offset commit and reads it from the compacted log
    Then offset bytes are <hex> and recovery reports leader epoch <leader_epoch> with topic identifier <output_topic>
    Examples:
      | expiry | input_topic | hex | leader_epoch | output_topic |
      | absent | true | 0004000000000000002a00000004056d65746100000000000f42400100100102030405060708090a0b0c0d0e0f10 | 4 | true |
      | absent | false | 0004000000000000002a00000004056d65746100000000000f424000 | 4 | false |
      | 9999999 | true | 0001000000000000002a00046d65746100000000000f4240000000000098967f | -1 | false |
