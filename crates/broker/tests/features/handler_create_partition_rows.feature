Feature: Partition growth validation preserves ordered errors and state

  Scenario: the same growth twice
    Given topic t has two partitions at replication factor one
    And the partition growth request contains these ordered topics
      | name | count | assignments |
      | t | 4 | null |
      | t | 4 | null |
    When CreatePartitions handles the growth with validate only false
    Then CreatePartitions returns exactly these rows and t still has two partitions
      | name | code | message |
      | t | 42 | Duplicate topic name. |

  Scenario: two different growths
    Given topic t has two partitions at replication factor one
    And the partition growth request contains these ordered topics
      | name | count | assignments |
      | t | 4 | null |
      | t | 6 | null |
    When CreatePartitions handles the growth with validate only false
    Then CreatePartitions returns exactly these rows and t still has two partitions
      | name | code | message |
      | t | 42 | Duplicate topic name. |

  Scenario: the same growth twice validate only
    Given topic t has two partitions at replication factor one
    And the partition growth request contains these ordered topics
      | name | count | assignments |
      | t | 4 | null |
      | t | 4 | null |
    When CreatePartitions handles the growth with validate only true
    Then CreatePartitions returns exactly these rows and t still has two partitions
      | name | code | message |
      | t | 42 | Duplicate topic name. |

  Scenario: duplicates precede other rows
    Given topic t has two partitions at replication factor one
    And the partition growth request contains these ordered topics
      | name | count | assignments |
      | u | 3 | null |
      | t | 4 | null |
      | t | 4 | null |
    When CreatePartitions handles the growth with validate only false
    Then CreatePartitions returns exactly these rows and t still has two partitions
      | name | code | message |
      | t | 42 | Duplicate topic name. |
      | u | 3 | null |

  Scenario: a count below the current one
    Given topic t has two partitions at replication factor one
    And the partition growth request contains these ordered topics
      | name | count | assignments |
      | t | 1 | null |
    When CreatePartitions handles the growth with validate only false
    Then CreatePartitions returns exactly these rows and t still has two partitions
      | name | code | message |
      | t | 37 | The topic t currently has 2 partition(s); 1 would not be an increase. |

  Scenario: too few explicit assignments
    Given topic t has two partitions at replication factor one
    And the partition growth request contains these ordered topics
      | name | count | assignments |
      | t | 4 | 1 |
    When CreatePartitions handles the growth with validate only false
    Then CreatePartitions returns exactly these rows and t still has two partitions
      | name | code | message |
      | t | 39 | Attempted to add 2 additional partition(s), but only 1 assignment(s) were specified. |
