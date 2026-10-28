Feature: tests soak probe

  Scenario: tests: vm rss is read in kibibytes
    Then soak probe tests: vm rss is read in kibibytes

  Scenario: tests: a status without vm rss has no resident set
    Then soak probe tests: a status without vm rss has no resident set

  Scenario: tests: series are the sample lines alone
    Then soak probe tests: series are the sample lines alone

  Scenario: tests: a metric sums over its label sets
    Then soak probe tests: a metric sums over its label sets

  Scenario: tests: an absent metric is zero
    Then soak probe tests: an absent metric is zero

  Scenario: tests: segment names yield their base offset
    Then soak probe tests: segment names yield their base offset

  Scenario: tests: directory usage sums the tree per top level entry
    Then soak probe tests: directory usage sums the tree per top level entry

  Scenario: tests: a partition directory is the topic a dash and an index
    Then soak probe tests: a partition directory is the topic a dash and an index

  Scenario: tests: usage without a topic leaves its partitions out
    Then soak probe tests: usage without a topic leaves its partitions out

  Scenario: tests: second half growth names what grew most
    Then soak probe tests: second half growth names what grew most

  Scenario: tests: segments are grouped by partition
    Then soak probe tests: segments are grouped by partition

  Scenario: tests: an implausible sample saturates above every ceiling
    Then soak probe tests: an implausible sample saturates above every ceiling

  Scenario: tests: a live process reports both readings
    Then soak probe tests: a live process reports both readings
