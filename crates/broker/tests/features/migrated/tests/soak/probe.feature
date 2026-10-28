Feature: Resource probes produce reliable operator measurements
  Scenario: tests: vm rss is read in kibibytes
    Given the kernel status reports VmRSS 262144 kB
    When the resource probe parses the resident-set statuses
    Then the resident set is 268435456 bytes

  Scenario: tests: a status without vm rss has no resident set
    Given kernel statuses omit VmRSS use unsupported pages or are empty
    When the resource probe parses the resident-set statuses
    Then all three statuses report no resident-set reading

  Scenario: tests: series are the sample lines alone
    Given a scrape contains five samples including labelled compactions and a prefix-neighbor metric
    When the probe counts samples in the scrape an empty body and metadata alone
    Then their metric cardinalities are five zero and zero

  Scenario: tests: a metric sums over its label sets
    Given a scrape contains five samples including labelled compactions and a prefix-neighbor metric
    When the probe sums compactions cleaner runs and compaction seconds
    Then compactions sum to 12 cleaner runs to 41 and the distinct seconds metric to 999

  Scenario: tests: an absent metric is zero
    Given a scrape contains five samples including labelled compactions and a prefix-neighbor metric
    When the probe reads the absent cleaner-failures metric
    Then the absent metric has value zero

  Scenario: tests: segment names yield their base offset
    Given segment filenames start at zero and 4096 beside an index and epoch checkpoint
    When the probe decodes segment base offsets
    Then only log files yield base offsets zero and 4096

  Scenario: tests: directory usage sums the tree per top level entry
    Given the log-directory tree contains regular files:
      | path            | bytes |
      | a               | 100   |
      | nested/b        | 250   |
      | nested/deeper/c  | 50    |
    When the probe reads total missing and top-level directory usage
    Then usage is 400 bytes in three files with a 100-byte file and 300-byte nested subtree

  Scenario: tests: a partition directory is the topic a dash and an index
    Given topic soak-verify has these candidate partition directories:
      | entry                |
      | soak-verify-0        |
      | soak-verify-12       |
      | soak-verify          |
      | soak-verify-         |
      | soak-verify-x        |
      | soak-verify-extra-0  |
      | soak-retention-0     |
      | __cluster_metadata-0 |
    When the probe identifies exact topic partition directories
    Then partition-directory membership is:
      | entry                | matches |
      | soak-verify-0        | true    |
      | soak-verify-12       | true    |
      | soak-verify          | false   |
      | soak-verify-         | false   |
      | soak-verify-x        | false   |
      | soak-verify-extra-0  | false   |
      | soak-retention-0     | false   |
      | __cluster_metadata-0 | false   |

  Scenario: tests: usage without a topic leaves its partitions out
    Given top-level log-directory usage is:
      | name                 | bytes | files |
      | soak-verify-0        | 1000  | 3     |
      | soak-retention-0     | 200   | 4     |
      | __cluster_metadata-0 | 30    | 5     |
      | meta.properties      | 4     | 1     |
    When the probe excludes soak-verify partitions and separately excludes an absent topic
    Then excluded usage is 234 bytes in ten files and absent-topic usage is 1234 bytes in thirteen

  Scenario: tests: second half growth names what grew most
    Given directory usage was sampled at these elapsed seconds:
      | seconds | entry   | bytes | files |
      | 0       | meta    | 0     | 1     |
      | 0       | offsets | 0     | 3     |
      | 40      | meta    | 900   | 1     |
      | 40      | offsets | 900   | 3     |
      | 50      | meta    | 1000  | 7     |
      | 50      | offsets | 500   | 3     |
      | 50      | gone    | 80    | 1     |
      | 50      | flat    | 10    | 1     |
      | 100     | meta    | 1200  | 9     |
      | 100     | offsets | 2500  | 3     |
      | 100     | new     | 300   | 2     |
      | 100     | flat    | 10    | 1     |
    When the probe ranks second-half growth with limits ten and one and checks an empty history
    Then the ranked directory growth is:
      | entry   | bytes | files |
      | offsets | 2000  | 0     |
      | new     | 300   | 2     |
      | meta    | 200   | 2     |
      | gone    | -80   | -1    |

  Scenario: tests: segments are grouped by partition
    Given the log-directory tree contains regular files:
      | path                              | bytes |
      | t-0/00000000000000000000.log        | 1     |
      | t-0/00000000000000000512.log        | 1     |
      | t-1/00000000000000000000.log        | 1     |
      | __meta/leader-epoch-checkpoint     | 1     |
    When the probe groups segment files by partition
    Then only t-0 and t-1 contain segments with bases zero-512 and zero respectively

  Scenario: tests: an implausible sample saturates above every ceiling
    Given sample readings include 1024 the maximum u64 and a count of seven
    When the probe converts the readings to trend-analysis numbers
    Then the readings are 1024 the maximum u32 and seven without wrapping

  Scenario: tests: a live process reports both readings
    Given the resource probe targets this live process
    When the probe reads live descriptors and resident memory
    Then the process reports positive descriptor and resident-memory readings
