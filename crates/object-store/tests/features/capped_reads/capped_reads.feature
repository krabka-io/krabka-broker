Feature: Buffered reads of archive objects are capped by size

  A broker reads a remote index of KIP-405 tiered storage whole into memory,
  under its configured index cap (crates/remote-storage/src/s3/fetch.rs). An
  auditor verifying a WORM archive reads each segment manifest the same way,
  under a cap of one mebibyte (crates/remote-storage/src/worm/verify/manifest_read.rs).
  The cap is the guard against a corrupt or hostile archive making the broker
  buffer an arbitrary object.

  Rule: An object larger than the cap is refused before any of it is read

    Scenario: A remote index exactly at the cap is read whole
      Given a remote index of 64 bytes has been archived
      When a broker reads the remote index with a cap of 64 bytes
      Then the broker receives the whole remote index

    Scenario: A remote index one byte over the cap is refused with its size and the cap
      Given a remote index of 64 bytes has been archived
      When a broker attempts to read the remote index with a cap of 63 bytes
      Then the read is refused because the remote index is 64 bytes, above the cap of 63 bytes

    Scenario: An oversized segment manifest is refused from its size alone
      Given a segment manifest of 2048 bytes has been archived
      When an auditor attempts to read the segment manifest with a cap of 1024 bytes
      Then the read is refused because the segment manifest is 2048 bytes, above the cap of 1024 bytes
      And the store was asked only for the size of the segment manifest

  Rule: A missing object is reported as not found, distinct from a store failure

    Scenario: A remote index that was never archived is reported as not found
      When a broker attempts to read a remote index that was never archived
      Then the read reports the remote index as not found

    Scenario: A store failure during a capped read is not reported as not found
      Given a remote index of 64 bytes has been archived
      And the store fails every read with a server error
      When a broker attempts to read the remote index with a cap of 64 bytes
      Then the read fails with a store error

  Rule: An object replaced by a larger one after its size check never slips past the cap

    Scenario: A larger replacement on a store with entity tags is refused as changed
      Given a remote index of 64 bytes has been archived
      And the remote index is replaced by one of 128 bytes after its size is checked
      When a broker attempts to read the remote index with a cap of 64 bytes
      Then the read is refused because the remote index changed after its size was checked

    Scenario: A larger replacement on a store without entity tags is refused as too large
      Given a remote index of 64 bytes has been archived to a store without entity tags
      And the remote index is replaced by one of 128 bytes after its size is checked
      When a broker attempts to read the remote index with a cap of 64 bytes
      Then the read is refused because the remote index is 128 bytes, above the cap of 64 bytes

  Rule: Only a store with entity tags pins the read to the object whose size was checked

    Scenario: A same-size replacement on a store with entity tags is refused as changed
      Given a remote index of 64 bytes has been archived
      And the remote index is replaced by one of 64 bytes after its size is checked
      When a broker attempts to read the remote index with a cap of 64 bytes
      Then the read is refused because the remote index changed after its size was checked

    Scenario: A smaller replacement on a store without entity tags is read in its place
      Given a remote index of 64 bytes has been archived to a store without entity tags
      And the remote index is replaced by one of 32 bytes after its size is checked
      When a broker reads the remote index with a cap of 64 bytes
      Then the broker receives the replacement remote index
