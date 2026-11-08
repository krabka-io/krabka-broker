Feature: Signed checkpoints attest the audit chain to an operator key

  A broker signs a checkpoint over its chain head with its Ed25519 audit
  key, named by a key id. An auditor holds the public keys they trust, by key
  id. A checkpoint proves the records before it only when its signature
  verifies under the trusted key with its key id, and only when the head it
  signed is the head the auditor recomputes. The chain rules are in
  ../chain_verification/chain_verification.feature.

  Rule: A checkpoint verifies only under the key that signed it

    Scenario: A checkpoint checked against a different key is refused
      Given a broker has written 3 audit events and then a signed checkpoint
      When an auditor verifies the partition trusting a different key under the same key id
      Then the verifier reports a bad signature at the checkpoint

    Scenario: A checkpoint whose key id is not trusted is refused
      Given a broker has written 3 audit events and then a signed checkpoint
      When an auditor verifies the partition trusting no key for its key id
      Then the verifier reports an untrusted key at the checkpoint

  Rule: A checkpoint commits the chain head of the events before it

    Scenario: An event rewritten under a checkpoint is caught by that checkpoint
      Given a broker has written 3 audit events and then a signed checkpoint
      And the stored event at sequence 2 has been rewritten
      When an auditor verifies the partition
      Then the verifier reports a chain head mismatch at the checkpoint

  Rule: Checkpoints signed before a key rotation stay verifiable

    Scenario: A partition signed across a key rotation verifies with both keys
      Given a broker has signed checkpoints before and after an audit key rotation
      When an auditor verifies the partition trusting the old and the new key
      Then the partition is fully attested over 4 events

    Scenario: A checkpoint signed with the retired key needs the retired key
      Given a broker has signed checkpoints before and after an audit key rotation
      When an auditor verifies the partition trusting only the new key
      Then the verifier reports an untrusted key at the checkpoint
