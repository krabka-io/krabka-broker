Feature: Object keys sit beneath a configured prefix

  An operator gives a bucket a prefix so that more than one cluster, or a
  backup archive beside a cluster, can share it. The broker writes diskless
  WAL objects beneath the prefix of its remote storage, and krabka backup and
  krabka restore read and write the archive beneath --archive-prefix. The
  prefix names one archive root however the operator spelled it, and keys
  under one root never touch the keys under another.

  Rule: A prefix names one archive root however it is spelled

    Scenario Outline: A WAL object is written beneath the root its prefix names
      When a broker writes the WAL object orders-0/segment.log under the prefix <prefix>
      Then the bucket holds exactly the key <key>

      Examples:
        | case                         | prefix     | key                            |
        | a bare prefix                | tier       | tier/orders-0/segment.log      |
        | a prefix copied with slashes | /tier/     | tier/orders-0/segment.log      |
        | a nested prefix              | prod/tier  | prod/tier/orders-0/segment.log |
        | an interior double slash     | prod//tier | prod/tier/orders-0/segment.log |

    Scenario: A prefix copied with surrounding spaces names the same root
      When a broker writes the WAL object orders-0/segment.log under a space-padded prefix /tier/
      Then the bucket holds exactly the key tier/orders-0/segment.log

  Rule: A prefix with no name in it means no prefix

    Scenario: A slash-only prefix leaves the object at the bucket root
      When a broker writes the WAL object orders-0/segment.log under the prefix /
      Then the bucket holds exactly the key orders-0/segment.log

    Scenario: An empty prefix leaves the object at the bucket root
      When a broker writes the WAL object orders-0/segment.log under an empty prefix
      Then the bucket holds exactly the key orders-0/segment.log

    Scenario: A prefix of only spaces leaves the object at the bucket root
      When a broker writes the WAL object orders-0/segment.log under a prefix of only spaces
      Then the bucket holds exactly the key orders-0/segment.log

    Scenario: A broker with no prefix keeps the object at the bucket root
      When a broker with no prefix writes the WAL object orders-0/segment.log
      Then the bucket holds exactly the key orders-0/segment.log

  Rule: Clusters that share a bucket under different archive roots never collide

    Scenario Outline: A second cluster archives the same object under its own root
      Given a cluster has archived a WAL object under the prefix <first>
      When a second cluster attempts a create-only write of the same WAL object under the prefix <second>
      Then the write is accepted
      And each cluster reads back its own object bytes

      Examples:
        | case                                | first | second |
        | different prefixes                  | east  | west   |
        | a prefix that extends another       | tier  | tier2  |

    Scenario Outline: A second cluster under the same archive root is refused
      Given a cluster has archived a WAL object under the prefix <first>
      When a second cluster attempts a create-only write of the same WAL object under the prefix <second>
      Then the write is refused because the object already exists
      And the first cluster still reads back its own object bytes

      Examples:
        | case                        | first | second |
        | the same prefix             | tier  | tier   |
        | two spellings of one root   | tier  | /tier/ |

  Rule: Listing a prefix returns only the keys beneath its whole archive root

    Scenario Outline: Listing a prefix leaves out a sibling root that shares its leading characters
      Given a bucket in the <store> store holds a WAL object under each of the prefixes tier, tier2
      When a broker lists the prefix tier
      Then the listing holds exactly tier/orders-0/segment.log

      Examples:
        | case                  | store            |
        | the test store        | in-memory        |
        | the development store | local-filesystem |

    Scenario: Listing a prefix includes a root nested beneath it
      Given a bucket holds a WAL object under each of the prefixes tier, tier/archive
      When a broker lists the prefix tier
      Then the listing holds exactly tier/archive/orders-0/segment.log, tier/orders-0/segment.log
