Feature: Share acknowledgement batches preserve ownership, rollback and delivery limits

  Scenario: A later ownership failure rolls back earlier acceptance
    Given share offsets 0 through 9 have delivery count 1, member owns the first 5 records and the first 0 records are accepted
    When member acknowledges these ordered batches with renewal disabled and maximum delivery count 2
      | first | last | types |
      | 0 | 4 | 1 |
      | 5 | 9 | 1 |
    Then acknowledgement returns code 121, start offset 0 and these remaining record states
      | first | last | state |
      | 0 | 9 | acquired |

  Scenario: A batch below the start offset is skipped
    Given share offsets 0 through 9 have delivery count 1, member owns the first 10 records and the first 5 records are accepted
    When member acknowledges these ordered batches with renewal disabled and maximum delivery count 2
      | first | last | types |
      | 0 | 2 | 1 |
    Then acknowledgement returns code 0, start offset 5 and these remaining record states
      | first | last | state |
      | 5 | 9 | acquired |

  Scenario: A crossing batch accepts only records above the start
    Given share offsets 0 through 9 have delivery count 1, member owns the first 10 records and the first 5 records are accepted
    When member acknowledges these ordered batches with renewal disabled and maximum delivery count 2
      | first | last | types |
      | 3 | 7 | 1 |
    Then acknowledgement returns code 0, start offset 8 and these remaining record states
      | first | last | state |
      | 8 | 9 | acquired |

  Scenario: Per-offset types are aligned across the start offset
    Given share offsets 0 through 9 have delivery count 1, member owns the first 10 records and the first 5 records are accepted
    When member acknowledges these ordered batches with renewal disabled and maximum delivery count 2
      | first | last | types |
      | 4 | 6 | 2,1,1 |
    Then acknowledgement returns code 0, start offset 7 and these remaining record states
      | first | last | state |
      | 7 | 9 | acquired |

  Scenario: A batch beyond acquired offsets rolls back earlier acceptance
    Given share offsets 0 through 9 have delivery count 1, member owns the first 10 records and the first 0 records are accepted
    When member acknowledges these ordered batches with renewal disabled and maximum delivery count 2
      | first | last | types |
      | 0 | 4 | 1 |
      | 8 | 12 | 1 |
    Then acknowledgement returns code 42, start offset 0 and these remaining record states
      | first | last | state |
      | 0 | 9 | acquired |

  Scenario: Release at the delivery limit archives all records
    Given share offsets 0 through 9 have delivery count 2, member owns the first 10 records and the first 0 records are accepted
    When member acknowledges these ordered batches with renewal disabled and maximum delivery count 2
      | first | last | types |
      | 0 | 9 | 2 |
    Then acknowledgement returns code 0, start offset 10 and these remaining record states
      | first | last | state |

  Scenario: Release below the delivery limit makes records available
    Given share offsets 0 through 9 have delivery count 1, member owns the first 10 records and the first 0 records are accepted
    When member acknowledges these ordered batches with renewal disabled and maximum delivery count 2
      | first | last | types |
      | 0 | 9 | 2 |
    Then acknowledgement returns code 0, start offset 0 and these remaining record states
      | first | last | state |
      | 0 | 9 | available |

  Scenario: Partial release at the limit advances the start offset
    Given share offsets 0 through 9 have delivery count 2, member owns the first 10 records and the first 0 records are accepted
    When member acknowledges these ordered batches with renewal disabled and maximum delivery count 2
      | first | last | types |
      | 0 | 4 | 2 |
    Then acknowledgement returns code 0, start offset 5 and these remaining record states
      | first | last | state |
      | 5 | 9 | acquired |

  Scenario: A later batch skips the run archived by an earlier batch
    Given share offsets 0 through 9 have delivery count 2, member owns the first 10 records and the first 0 records are accepted
    When member acknowledges these ordered batches with renewal disabled and maximum delivery count 2
      | first | last | types |
      | 0 | 9 | 2 |
      | 0 | 9 | 1 |
    Then acknowledgement returns code 0, start offset 10 and these remaining record states
      | first | last | state |
