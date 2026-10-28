Feature: Topic deletion separates Describe visibility from Delete authorization

  Background: An existing topic
    Given alice may create topics and topic t exists with its captured UUID

  Scenario: Delete permits UUID deletion and reveals the name
    Given alice has these exact topic grants for deletion
      | topic | operation |
      | t | Delete |
    When alice deletes topic reference UUID:t together with unknown UUID 08080808080808080808080808080808
    Then the complete DeleteTopics rows are these and topic t remains false
      | name | UUID | code |
      | t | t | 0 |
      | null | 08080808080808080808080808080808 | 100 |

  Scenario: Describe reveals a denied UUID name
    Given alice has these exact topic grants for deletion
      | topic | operation |
      | t | Describe |
    When alice deletes topic reference UUID:t together with unknown UUID 08080808080808080808080808080808
    Then the complete DeleteTopics rows are these and topic t remains true
      | name | UUID | code |
      | t | t | 29 |
      | null | 08080808080808080808080808080808 | 100 |

  Scenario: Without Describe denied UUID deletion hides the name
    Given alice has these exact topic grants for deletion
      | topic | operation |
    When alice deletes topic reference UUID:t together with unknown UUID 08080808080808080808080808080808
    Then the complete DeleteTopics rows are these and topic t remains true
      | name | UUID | code |
      | null | t | 29 |
      | null | 08080808080808080808080808080808 | 100 |

  Scenario: Describe reveals that a named topic is missing
    Given alice has these exact topic grants for deletion
      | topic | operation |
      | missing | Describe |
    When alice deletes topic reference missing together with unknown UUID 08080808080808080808080808080808
    Then the complete DeleteTopics rows are these and topic t remains true
      | name | UUID | code |
      | missing | zero | 3 |
      | null | 08080808080808080808080808080808 | 100 |

  Scenario: Without Describe a missing name is not disclosed
    Given alice has these exact topic grants for deletion
      | topic | operation |
    When alice deletes topic reference missing together with unknown UUID 08080808080808080808080808080808
    Then the complete DeleteTopics rows are these and topic t remains true
      | name | UUID | code |
      | missing | zero | 29 |
      | null | 08080808080808080808080808080808 | 100 |

  Scenario: Describe without Delete cannot remove an existing named topic
    Given alice has these exact topic grants for deletion
      | topic | operation |
      | t | Describe |
    When alice deletes topic reference t together with unknown UUID 08080808080808080808080808080808
    Then the complete DeleteTopics rows are these and topic t remains true
      | name | UUID | code |
      | t | zero | 29 |
      | null | 08080808080808080808080808080808 | 100 |
