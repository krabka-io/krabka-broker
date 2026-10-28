Feature: CreateTopics check precedence, duplicate names and request partition limits

  Background: Existing topic
    Given a single allow-all broker already holds topic t with one partition and replication factor one

  Scenario: A duplicated name creates nothing
    When CreateTopics processes these ordered rows with validate only false
      | topic | partitions | replication factor | configs | assignments |
      | n | 1 | 1 | none | none |
      | n | 1 | 1 | none | none |
    Then the full CreateTopics response has these ordered rows and the noninternal topics are t
      | topic | code | message |
      | n | 42 | Duplicate topic name. |

  Scenario: A duplicated name is also invalid in a dry run
    When CreateTopics processes these ordered rows with validate only true
      | topic | partitions | replication factor | configs | assignments |
      | n | 1 | 1 | none | none |
      | n | 1 | 1 | none | none |
    Then the full CreateTopics response has these ordered rows and the noninternal topics are t
      | topic | code | message |
      | n | 42 | Duplicate topic name. |

  Scenario: Controller rows precede duplicate-name refusals
    When CreateTopics processes these ordered rows with validate only false
      | topic | partitions | replication factor | configs | assignments |
      | n | 1 | 1 | none | none |
      | u | 1 | 1 | none | none |
      | n | 1 | 1 | none | none |
    Then the full CreateTopics response has these ordered rows and the noninternal topics are t,u
      | topic | code | message |
      | u | 0 | null |
      | n | 42 | Duplicate topic name. |

  Scenario: Existing topics precede config validation
    When CreateTopics processes these ordered rows with validate only false
      | topic | partitions | replication factor | configs | assignments |
      | t | 1 | 1 | retention.ms:abc | none |
    Then the full CreateTopics response has these ordered rows and the noninternal topics are t
      | topic | code | message |
      | t | 36 | Topic 't' already exists. |

  Scenario: Existing topics precede partition count validation
    When CreateTopics processes these ordered rows with validate only false
      | topic | partitions | replication factor | configs | assignments |
      | t | 0 | 1 | none | none |
    Then the full CreateTopics response has these ordered rows and the noninternal topics are t
      | topic | code | message |
      | t | 36 | Topic 't' already exists. |

  Scenario: Existing topics precede replication factor validation
    When CreateTopics processes these ordered rows with validate only false
      | topic | partitions | replication factor | configs | assignments |
      | t | 1 | 3 | none | none |
    Then the full CreateTopics response has these ordered rows and the noninternal topics are t
      | topic | code | message |
      | t | 36 | Topic 't' already exists. |

  Scenario: Existing topics precede config validation in dry runs
    When CreateTopics processes these ordered rows with validate only true
      | topic | partitions | replication factor | configs | assignments |
      | t | 1 | 1 | retention.ms:abc | none |
    Then the full CreateTopics response has these ordered rows and the noninternal topics are t
      | topic | code | message |
      | t | 36 | Topic 't' already exists. |

  Scenario: Null config values are invalid
    When CreateTopics processes these ordered rows with validate only false
      | topic | partitions | replication factor | configs | assignments |
      | n | 1 | 1 | retention.ms:null | none |
    Then the full CreateTopics response has these ordered rows and the noninternal topics are t
      | topic | code | message |
      | n | 40 | Null value not supported for topic configs: retention.ms |

  Scenario: All null configs are named in dry-run refusal
    When CreateTopics processes these ordered rows with validate only true
      | topic | partitions | replication factor | configs | assignments |
      | n | 1 | 1 | retention.ms:null;segment.ms:60000;cleanup.policy:null | none |
    Then the full CreateTopics response has these ordered rows and the noninternal topics are t
      | topic | code | message |
      | n | 40 | Null value not supported for topic configs: retention.ms,cleanup.policy |

  Scenario: Explicit assignments require sentinel replication factor
    When CreateTopics processes these ordered rows with validate only false
      | topic | partitions | replication factor | configs | assignments |
      | m | -1 | 2 | none | 0:1 |
    Then the full CreateTopics response has these ordered rows and the noninternal topics are t
      | topic | code | message |
      | m | 42 | A manual partition assignment was specified, but replication factor was not set to -1. |

  Scenario: One topic over the partition request limit refuses creation
    When CreateTopics processes these ordered rows with validate only false
      | topic | partitions | replication factor | configs | assignments |
      | a | 10001 | 1 | none | none |
    Then the full CreateTopics response has these ordered rows and the noninternal topics are t
      | topic | code | message |
      | a | 44 | Excessively large number of partitions per request. |

  Scenario: The aggregate partition limit refuses every original row including duplicates
    When CreateTopics processes these ordered rows with validate only false
      | topic | partitions | replication factor | configs | assignments |
      | a | 6000 | 1 | none | none |
      | d | 1 | 1 | none | none |
      | b | 6000 | 1 | none | none |
      | d | 1 | 1 | none | none |
    Then the full CreateTopics response has these ordered rows and the noninternal topics are t
      | topic | code | message |
      | a | 44 | Excessively large number of partitions per request. |
      | d | 44 | Excessively large number of partitions per request. |
      | b | 44 | Excessively large number of partitions per request. |
      | d | 44 | Excessively large number of partitions per request. |
