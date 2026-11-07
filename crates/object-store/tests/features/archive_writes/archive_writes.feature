Feature: Archive writes are write-once and return a receipt

  The WORM archive of KIP-405 tiered storage, in crates/remote-storage, writes
  every segment object and segment manifest create-only and records each
  write's receipt in the manifest. A create-only write must never replace an
  object, and the receipt must describe exactly the bytes that were stored.

  Rule: A create-only write refuses a key that already holds an object

    Scenario Outline: A second create-only write of a tiered segment is refused
      Given a tiered segment has been archived to the <store> store
      When a broker attempts a create-only write of other bytes to the same key
      Then the write is refused because the object already exists
      And the key still holds the first archived segment

      Examples:
        | case                         | store            |
        | the test store               | in-memory        |
        | the development store        | local-filesystem |

    Scenario: A create-only write to a fresh key is accepted
      When a broker attempts a create-only write of a tiered segment to a fresh key
      Then the write is accepted
      And the key holds the new bytes

    Scenario: An overwrite replaces the object at an archived key
      Given a tiered segment has been archived
      When a broker attempts an overwrite of other bytes to the same key
      Then the write is accepted
      And the key holds the new bytes

  Rule: A large create-only upload reserves its key before it streams any part

    Scenario: A large create-only upload to an archived key is refused
      Given a tiered segment has been archived
      When a broker attempts a create-only upload of a segment file at the multipart threshold to the same key
      Then the write is refused because the object already exists
      And the key still holds the first archived segment

  Rule: Only an upload below the multipart threshold is guarded by an atomic create

    Scenario Outline: The protection of a create-only upload follows its size
      Given segment files of 1024 bytes and more upload in parts
      When a broker uploads a segment file of <size> bytes create-only
      Then the receipt says the object is protected by <protection>

      Examples:
        | case                         | size | protection       |
        | one byte below the threshold | 1023 | atomic-create    |
        | exactly at the threshold     | 1024 | bucket-retention |

  Rule: A digested write reports the SHA-256 and size of exactly the bytes it stored

    Scenario Outline: A digested segment write reports its digest and size
      When a broker writes the segment <payload> with a digest
      Then the receipt reports a size of <size> bytes
      And the receipt reports the SHA-256 <digest>

      Examples:
        | case                    | payload | size | digest                                                           |
        | the FIPS 180-2 vector   | abc     | 3    | ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad |
        | an empty segment        | empty   | 0    | e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855 |

    Scenario: A digested multipart upload covers every part of the file
      Given segment files of 1024 bytes and more upload in parts
      When a broker uploads a segment file of one million a characters with a digest
      Then the receipt reports the SHA-256 cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0

    Scenario: An undigested write carries no digest
      When a broker writes the segment abc without a digest
      Then the receipt carries no digest

  Rule: The receipt carries the entity tag the store reports for the stored object

    Scenario: A segment write returns the entity tag of the stored object
      When a broker writes the segment abc with a digest
      Then the receipt carries the entity tag the store reports for the key
