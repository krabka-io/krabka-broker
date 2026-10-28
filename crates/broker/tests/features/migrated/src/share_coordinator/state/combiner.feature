Feature: Kafka share-delivery range combination
  Overlapping ranges are resolved by delivery count and then delivery state.
  Equal adjacent metadata coalesces; retained-start clipping never resurrects
  older offsets. These include the PersisterStateBatchCombiner issue-935 cases.

  Scenario: Empty histories remain empty
    Given the stored share-delivery ranges are:
      | first | last | delivery | attempts |
    And the incoming share-delivery ranges are:
      | first | last | delivery | attempts |
    And the retained share-delivery log starts at offset 0
    When the share coordinator combines the stored and incoming delivery ranges
    Then the resulting share-delivery ranges are:
      | first | last | delivery | attempts |

  Scenario: A lone incoming range passes through
    Given the stored share-delivery ranges are:
      | first | last | delivery | attempts |
    And the incoming share-delivery ranges are:
      | first | last | delivery | attempts |
      | 100 | 109 | available | 1 |
    And the retained share-delivery log starts at offset -1
    When the share coordinator combines the stored and incoming delivery ranges
    Then the resulting share-delivery ranges are:
      | first | last | delivery | attempts |
      | 100 | 109 | available | 1 |

  Scenario: Disjoint ranges remain separate and sort by offset
    Given the stored share-delivery ranges are:
      | first | last | delivery | attempts |
      | 110 | 119 | available | 1 |
    And the incoming share-delivery ranges are:
      | first | last | delivery | attempts |
      | 100 | 104 | acknowledged | 1 |
    And the retained share-delivery log starts at offset -1
    When the share coordinator combines the stored and incoming delivery ranges
    Then the resulting share-delivery ranges are:
      | first | last | delivery | attempts |
      | 100 | 104 | acknowledged | 1 |
      | 110 | 119 | available | 1 |

  Scenario: Adjacent ranges with equal delivery metadata coalesce
    Given the stored share-delivery ranges are:
      | first | last | delivery | attempts |
      | 100 | 104 | available | 1 |
    And the incoming share-delivery ranges are:
      | first | last | delivery | attempts |
      | 105 | 109 | available | 1 |
    And the retained share-delivery log starts at offset -1
    When the share coordinator combines the stored and incoming delivery ranges
    Then the resulting share-delivery ranges are:
      | first | last | delivery | attempts |
      | 100 | 109 | available | 1 |

  Scenario: A greater delivery count wins overlapping offsets
    Given the stored share-delivery ranges are:
      | first | last | delivery | attempts |
      | 100 | 109 | available | 1 |
    And the incoming share-delivery ranges are:
      | first | last | delivery | attempts |
      | 103 | 105 | available | 2 |
    And the retained share-delivery log starts at offset -1
    When the share coordinator combines the stored and incoming delivery ranges
    Then the resulting share-delivery ranges are:
      | first | last | delivery | attempts |
      | 100 | 102 | available | 1 |
      | 103 | 105 | available | 2 |
      | 106 | 109 | available | 1 |

  Scenario: Acknowledgement wins an equal-count overlap
    Given the stored share-delivery ranges are:
      | first | last | delivery | attempts |
      | 100 | 109 | acknowledged | 1 |
    And the incoming share-delivery ranges are:
      | first | last | delivery | attempts |
      | 100 | 109 | available | 1 |
    And the retained share-delivery log starts at offset -1
    When the share coordinator combines the stored and incoming delivery ranges
    Then the resulting share-delivery ranges are:
      | first | last | delivery | attempts |
      | 100 | 109 | acknowledged | 1 |

  Scenario: The retained start drops old offsets and clips surviving ranges
    Given the stored share-delivery ranges are:
      | first | last | delivery | attempts |
      | 90 | 99 | available | 1 |
      | 100 | 109 | available | 1 |
    And the incoming share-delivery ranges are:
      | first | last | delivery | attempts |
      | 95 | 104 | acknowledged | 1 |
    And the retained share-delivery log starts at offset 103
    When the share coordinator combines the stored and incoming delivery ranges
    Then the resulting share-delivery ranges are:
      | first | last | delivery | attempts |
      | 103 | 104 | acknowledged | 1 |
      | 105 | 109 | available | 1 |

  Scenario: An acknowledged suffix splits an available stored range
    Given the stored share-delivery ranges are:
      | first | last | delivery | attempts |
      | 0 | 9 | available | 1 |
    And the incoming share-delivery ranges are:
      | first | last | delivery | attempts |
      | 5 | 9 | acknowledged | 1 |
    And the retained share-delivery log starts at offset 0
    When the share coordinator combines the stored and incoming delivery ranges
    Then the resulting share-delivery ranges are:
      | first | last | delivery | attempts |
      | 0 | 4 | available | 1 |
      | 5 | 9 | acknowledged | 1 |

  Scenario: A lower delivery count cannot replace stored attempts
    Given the stored share-delivery ranges are:
      | first | last | delivery | attempts |
      | 0 | 9 | available | 2 |
    And the incoming share-delivery ranges are:
      | first | last | delivery | attempts |
      | 0 | 9 | acknowledged | 1 |
    And the retained share-delivery log starts at offset 0
    When the share coordinator combines the stored and incoming delivery ranges
    Then the resulting share-delivery ranges are:
      | first | last | delivery | attempts |
      | 0 | 9 | available | 2 |

  Scenario: A higher delivery state replaces an equal-count range
    Given the stored share-delivery ranges are:
      | first | last | delivery | attempts |
      | 0 | 9 | available | 1 |
    And the incoming share-delivery ranges are:
      | first | last | delivery | attempts |
      | 0 | 9 | acknowledged | 1 |
    And the retained share-delivery log starts at offset 0
    When the share coordinator combines the stored and incoming delivery ranges
    Then the resulting share-delivery ranges are:
      | first | last | delivery | attempts |
      | 0 | 9 | acknowledged | 1 |

  Scenario: A stored range is clipped even without incoming ranges
    Given the stored share-delivery ranges are:
      | first | last | delivery | attempts |
      | 0 | 9 | available | 1 |
    And the incoming share-delivery ranges are:
      | first | last | delivery | attempts |
    And the retained share-delivery log starts at offset 5
    When the share coordinator combines the stored and incoming delivery ranges
    Then the resulting share-delivery ranges are:
      | first | last | delivery | attempts |
      | 5 | 9 | available | 1 |

  Scenario: Acknowledged adjacent ranges coalesce from offset zero
    Given the stored share-delivery ranges are:
      | first | last | delivery | attempts |
      | 0 | 4 | acknowledged | 1 |
    And the incoming share-delivery ranges are:
      | first | last | delivery | attempts |
      | 5 | 9 | acknowledged | 1 |
    And the retained share-delivery log starts at offset 0
    When the share coordinator combines the stored and incoming delivery ranges
    Then the resulting share-delivery ranges are:
      | first | last | delivery | attempts |
      | 0 | 9 | acknowledged | 1 |

  Scenario: An inner archived range splits its stored envelope in three
    Given the stored share-delivery ranges are:
      | first | last | delivery | attempts |
      | 0 | 9 | available | 1 |
    And the incoming share-delivery ranges are:
      | first | last | delivery | attempts |
      | 3 | 5 | archived | 3 |
    And the retained share-delivery log starts at offset -1
    When the share coordinator combines the stored and incoming delivery ranges
    Then the resulting share-delivery ranges are:
      | first | last | delivery | attempts |
      | 0 | 2 | available | 1 |
      | 3 | 5 | archived | 3 |
      | 6 | 9 | available | 1 |

  Scenario: A whole incoming range below the retained start is dropped
    Given the stored share-delivery ranges are:
      | first | last | delivery | attempts |
    And the incoming share-delivery ranges are:
      | first | last | delivery | attempts |
      | 0 | 9 | available | 1 |
      | 20 | 29 | available | 1 |
    And the retained share-delivery log starts at offset 20
    When the share coordinator combines the stored and incoming delivery ranges
    Then the resulting share-delivery ranges are:
      | first | last | delivery | attempts |
      | 20 | 29 | available | 1 |
