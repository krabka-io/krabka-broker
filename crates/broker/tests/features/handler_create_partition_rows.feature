Feature: Partition growth validation preserves ordered errors and state

  Rule: A topic named twice in one request is refused and does not grow

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

  Rule: Duplicate-name rows come before the rows of the other topics

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

  Rule: Each remaining topic answers in request order

    Scenario: two checked topics answer in the order they were requested
      Given topic t has two partitions at replication factor one
      And the partition growth request contains these ordered topics
        | name | count | assignments |
        | u | 3 | null |
        | t | 2 | null |
      When CreatePartitions handles the growth with validate only false
      Then CreatePartitions returns exactly these rows and t still has two partitions
        | name | code | message |
        | u | 3 | null |
        | t | 37 | Topic already has 2 partition(s). |

  Rule: The new partition count must be larger than the current one

    Scenario: a count below the current one
      Given topic t has two partitions at replication factor one
      And the partition growth request contains these ordered topics
        | name | count | assignments |
        | t | 1 | null |
      When CreatePartitions handles the growth with validate only false
      Then CreatePartitions returns exactly these rows and t still has two partitions
        | name | code | message |
        | t | 37 | The topic t currently has 2 partition(s); 1 would not be an increase. |

    Scenario: a count equal to the current one
      Given topic t has two partitions at replication factor one
      And the partition growth request contains these ordered topics
        | name | count | assignments |
        | t | 2 | null |
      When CreatePartitions handles the growth with validate only false
      Then CreatePartitions returns exactly these rows and t still has two partitions
        | name | code | message |
        | t | 37 | Topic already has 2 partition(s). |

  Rule: Explicit assignments name every new partition

    Scenario: too few explicit assignments
      Given topic t has two partitions at replication factor one
      And the partition growth request contains these ordered topics
        | name | count | assignments |
        | t | 4 | 1 |
      When CreatePartitions handles the growth with validate only false
      Then CreatePartitions returns exactly these rows and t still has two partitions
        | name | code | message |
        | t | 39 | Attempted to add 2 additional partition(s), but only 1 assignment(s) were specified. |

  Rule: A dry run answers the verdict and adds no partition

    Scenario: a valid growth in a dry run adds nothing
      Given topic t has two partitions at replication factor one
      And the partition growth request contains these ordered topics
        | name | count | assignments |
        | t | 4 | null |
      When CreatePartitions handles the growth with validate only true
      Then CreatePartitions returns exactly these rows and t still has two partitions
        | name | code | message |
        | t | 0 | null |
