# Quoted <empty> denotes a zero-length Kafka string; an empty created-topic list contains no names.
Feature: Share offset summaries, lag computation and known topic validation

  Background: Known topic metadata
    Given orders UUID d5c0 and audit UUID a0d1 each have four partitions

  Scenario: Lag subtracts start offset and completed deliveries
    Given share state stores these partition summaries
      | UUID | partition | state epoch | leader epoch | start | completed |
      | d5c0 | 0 | 1 | 4 | 21 | 10 |
    Given the end offset lookup for orders partition 0 returns 40
    When share offset description for group "g" requests these topic partitions
      | topic | partitions |
      | orders | 0 |
    Then the complete ordered share offset topic rows are
      | topic | UUID | partition | start | leader epoch | lag | code | message |
      | orders | d5c0 | 0 | 21 | 4 | 9 | 0 | null |

  Scenario: Missing partition state has no data or lag
    When share offset description for group "g" requests these topic partitions
      | topic | partitions |
      | orders | 1 |
    Then the complete ordered share offset topic rows are
      | topic | UUID | partition | start | leader epoch | lag | code | message |
      | orders | d5c0 | 1 | -1 | 0 | -1 | 0 | null |

  Scenario: Uninitialized delivery count suppresses lag
    Given share state stores these partition summaries
      | UUID | partition | state epoch | leader epoch | start | completed |
      | d5c0 | 0 | 1 | 2 | 5 | -1 |
    When share offset description for group "g" requests these topic partitions
      | topic | partitions |
      | orders | 0 |
    Then the complete ordered share offset topic rows are
      | topic | UUID | partition | start | leader epoch | lag | code | message |
      | orders | d5c0 | 0 | 5 | 2 | -1 | 0 | null |

  Scenario: State read failure has no data and no partition error
    Given share state stores these partition summaries
      | UUID | partition | state epoch | leader epoch | start | completed |
      | d5c0 | 0 | error | none | none | none |
    When share offset description for group "g" requests these topic partitions
      | topic | partitions |
      | orders | 0 |
    Then the complete ordered share offset topic rows are
      | topic | UUID | partition | start | leader epoch | lag | code | message |
      | orders | d5c0 | 0 | -1 | 0 | -1 | 0 | null |

  Scenario: Failed end lookup carries its own error
    Given share state stores these partition summaries
      | UUID | partition | state epoch | leader epoch | start | completed |
      | d5c0 | 0 | 1 | 4 | 21 | 10 |
    When share offset description for group "g" requests these topic partitions
      | topic | partitions |
      | orders | 0 |
    Then the complete ordered share offset topic rows are
      | topic | UUID | partition | start | leader epoch | lag | code | message |
      | orders | d5c0 | 0 | 0 | 0 | -1 | 13 | The server disconnected before a response was received. |

  Scenario: Unknown topics follow known topics
    When share offset description for group "g" requests these topic partitions
      | topic | partitions |
      | gone | 3,5 |
      | orders | 1 |
    Then the complete ordered share offset topic rows are
      | topic | UUID | partition | start | leader epoch | lag | code | message |
      | orders | d5c0 | 1 | -1 | 0 | -1 | 0 | null |
      | gone | 0 | 3 | -1 | 0 | -1 | 0 | null |
      | gone | 0 | 5 | -1 | 0 | -1 | 0 | null |

  Scenario: Unknown topics with empty partitions still have a topic row
    When share offset description for group "g" requests these topic partitions
      | topic | partitions |
      | gone | none |
    Then the complete ordered share offset topic rows are
      | topic | UUID | partition | start | leader epoch | lag | code | message |
      | gone | 0 | none | none | none | none | none | none |

  Scenario: Unknown topics need no group identity
    When share offset description for group "<empty>" requests these topic partitions
      | topic | partitions |
      | gone | 0 |
    Then the complete ordered share offset topic rows are
      | topic | UUID | partition | start | leader epoch | lag | code | message |
      | gone | 0 | 0 | -1 | 0 | -1 | 0 | null |

  Scenario: Repeated topics merge and repeated partitions collapse in first-seen order
    When share offset description for group "g" requests these topic partitions
      | topic | partitions |
      | orders | 1,0,1 |
      | audit | 0 |
      | orders | 2 |
    Then the complete ordered share offset topic rows are
      | topic | UUID | partition | start | leader epoch | lag | code | message |
      | orders | d5c0 | 1 | -1 | 0 | -1 | 0 | null |
      | orders | d5c0 | 0 | -1 | 0 | -1 | 0 | null |
      | orders | d5c0 | 2 | -1 | 0 | -1 | 0 | null |
      | audit | a0d1 | 0 | -1 | 0 | -1 | 0 | null |

  Scenario: An empty partition list for a known topic fails the group
    When share offset description for group "g" requests these topic partitions
      | topic | partitions |
      | orders | 0 |
      | orders | none |
    Then share offset description fails the whole group with UNKNOWN_SERVER_ERROR and Kafka's default message

  Scenario: A negative known partition fails the group
    When share offset description for group "g" requests these topic partitions
      | topic | partitions |
      | orders | -1 |
    Then share offset description fails the whole group with UNKNOWN_SERVER_ERROR and Kafka's default message

  Scenario: An empty group identity fails a known topic
    When share offset description for group "<empty>" requests these topic partitions
      | topic | partitions |
      | orders | 0 |
    Then share offset description fails the whole group with UNKNOWN_SERVER_ERROR and Kafka's default message
