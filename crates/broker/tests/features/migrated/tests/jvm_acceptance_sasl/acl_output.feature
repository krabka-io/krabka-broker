Feature: tests jvm acceptance sasl acl output

  Scenario: tests: every entry is flattened onto the resource it was printed under
    Then jvm acceptance sasl acl output tests: every entry is flattened onto the resource it was printed under

  Scenario: tests: the resources may be printed in any order
    Then jvm acceptance sasl acl output tests: the resources may be printed in any order

  Scenario: tests: a denied host is read off the entry that carries it
    Then jvm acceptance sasl acl output tests: a denied host is read off the entry that carries it

  Scenario: tests: entries printed before any resource header are dropped
    Then jvm acceptance sasl acl output tests: entries printed before any resource header are dropped

  Scenario: tests: an empty listing is an empty set
    Then jvm acceptance sasl acl output tests: an empty listing is an empty set
