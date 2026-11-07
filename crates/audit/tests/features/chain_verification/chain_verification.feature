Feature: An auditor verifies the hash chain of an audit partition offline

  krabka-audit verify reads one broker's __krabka_audit partition from disk,
  with no recovery and no truncation, and recomputes its hash chain. Each
  record carries its sequence number and the chain head before it, so a
  record that was rewritten, removed, moved or repeated shows up at the first
  record whose link no longer holds. The verifier stops there and names that
  sequence number. Checkpoint signatures are in
  ../checkpoint_signatures/checkpoint_signatures.feature.

  Rule: An unaltered chain under a signed checkpoint is fully attested

    Scenario: A partition whose every event is under a checkpoint verifies clean
      Given a broker has written 5 audit events and then a signed checkpoint
      When an auditor verifies the partition
      Then the partition is fully attested over 5 events

  Rule: Verification stops at the first record whose chain link does not hold

    Scenario Outline: An altered history is reported at the first broken link
      Given a broker has written 5 audit events
      And the stored event at sequence 2 has been <alteration>
      When an auditor verifies the partition
      Then the verifier reports a <break> at sequence <seq>

      Examples:
        | case                                       | alteration | break        | seq |
        | a rewrite shows at the next link           | rewritten  | broken link  | 3   |
        | a removed event leaves a gap               | removed    | sequence gap | 3   |
        | a swapped pair shows at the first one      | swapped    | sequence gap | 3   |
        | a repeated event is out of sequence        | repeated   | sequence gap | 2   |

  Rule: Events after the last signed checkpoint are reported as unattested

    Scenario: Events written after the last checkpoint are counted as unattested
      Given a broker has written 3 audit events and then a signed checkpoint
      And it has written 2 more audit events
      When an auditor verifies the partition
      Then the chain is continuous with 2 of 5 events unattested

    Scenario: A partition without a checkpoint is unattested throughout
      Given a broker has written 5 audit events
      When an auditor verifies the partition
      Then the chain is continuous with 5 of 5 events unattested

  Rule: A declared loss of records is reported without breaking the chain

    Scenario: A records-lost marker is reported as a loss
      Given a broker has declared 3 lost audit records in its chain
      When an auditor verifies the partition
      Then the verifier reports a loss of 3 records
      And the chain is continuous
