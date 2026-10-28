Feature: Share acknowledgement validation precedes authorization and metadata checks

  Scenario: Authorized valid acknowledgements settle acquired records
    Given reader acquired offsets 0 through 2 of ack-order partition 0 in separate fetch and acknowledge groups
    When principal reader sends these acknowledgement batches for partition 0 through both share APIs
      | first | last | types |
      | 0 | 2 | 1 |
    Then the share API rows and remaining acquired records are exactly
      | API | partition | error | acknowledgement error | states |
      | ShareFetch | 0 | 0 | 0 | none |
      | ShareAcknowledge | 0 | 0 | 0 | none |

  Scenario: Topic denial precedes state mutation
    Given reader acquired offsets 0 through 2 of ack-order partition 0 in separate fetch and acknowledge groups
    When principal denied sends these acknowledgement batches for partition 0 through both share APIs
      | first | last | types |
      | 0 | 2 | 1 |
    Then the share API rows and remaining acquired records are exactly
      | API | partition | error | acknowledgement error | states |
      | ShareFetch | 0 | 29 | 29 | acquired,acquired,acquired |
      | ShareAcknowledge | 0 | 29 | 0 | acquired,acquired,acquired |

  Scenario: Batch validation precedes acknowledge ACL checks
    Given reader acquired offsets 0 through 2 of ack-order partition 0 in separate fetch and acknowledge groups
    When principal denied sends these acknowledgement batches for partition 0 through both share APIs
      | first | last | types |
      | 2 | 0 | 1 |
    Then the share API rows and remaining acquired records are exactly
      | API | partition | error | acknowledgement error | states |
      | ShareFetch | 0 | 29 | 42 | acquired,acquired,acquired |
      | ShareAcknowledge | 0 | 42 | 0 | acquired,acquired,acquired |

  Scenario: Reversed offsets are invalid
    Given reader acquired offsets 0 through 2 of ack-order partition 0 in separate fetch and acknowledge groups
    When principal reader sends these acknowledgement batches for partition 0 through both share APIs
      | first | last | types |
      | 2 | 0 | 1 |
    Then the share API rows and remaining acquired records are exactly
      | API | partition | error | acknowledgement error | states |
      | ShareFetch | 0 | 0 | 42 | acquired,acquired,acquired |
      | ShareAcknowledge | 0 | 42 | 0 | acquired,acquired,acquired |

  Scenario: Overlapping batches are invalid
    Given reader acquired offsets 0 through 2 of ack-order partition 0 in separate fetch and acknowledge groups
    When principal reader sends these acknowledgement batches for partition 0 through both share APIs
      | first | last | types |
      | 0 | 1 | 1 |
      | 0 | 2 | 1 |
    Then the share API rows and remaining acquired records are exactly
      | API | partition | error | acknowledgement error | states |
      | ShareFetch | 0 | 0 | 42 | acquired,acquired,acquired |
      | ShareAcknowledge | 0 | 42 | 0 | acquired,acquired,acquired |

  Scenario: A missing acknowledge type is invalid
    Given reader acquired offsets 0 through 2 of ack-order partition 0 in separate fetch and acknowledge groups
    When principal reader sends these acknowledgement batches for partition 0 through both share APIs
      | first | last | types |
      | 0 | 2 | none |
    Then the share API rows and remaining acquired records are exactly
      | API | partition | error | acknowledgement error | states |
      | ShareFetch | 0 | 0 | 42 | acquired,acquired,acquired |
      | ShareAcknowledge | 0 | 42 | 0 | acquired,acquired,acquired |

  Scenario: Per-offset type counts must match the range
    Given reader acquired offsets 0 through 2 of ack-order partition 0 in separate fetch and acknowledge groups
    When principal reader sends these acknowledgement batches for partition 0 through both share APIs
      | first | last | types |
      | 0 | 2 | 1,1 |
    Then the share API rows and remaining acquired records are exactly
      | API | partition | error | acknowledgement error | states |
      | ShareFetch | 0 | 0 | 42 | acquired,acquired,acquired |
      | ShareAcknowledge | 0 | 42 | 0 | acquired,acquired,acquired |

  Scenario: Unknown acknowledge types are invalid
    Given reader acquired offsets 0 through 2 of ack-order partition 0 in separate fetch and acknowledge groups
    When principal reader sends these acknowledgement batches for partition 0 through both share APIs
      | first | last | types |
      | 0 | 2 | 5 |
    Then the share API rows and remaining acquired records are exactly
      | API | partition | error | acknowledgement error | states |
      | ShareFetch | 0 | 0 | 42 | acquired,acquired,acquired |
      | ShareAcknowledge | 0 | 42 | 0 | acquired,acquired,acquired |

  Scenario: Renew requires the renewal request flag
    Given reader acquired offsets 0 through 2 of ack-order partition 0 in separate fetch and acknowledge groups
    When principal reader sends these acknowledgement batches for partition 0 through both share APIs
      | first | last | types |
      | 0 | 2 | 4 |
    Then the share API rows and remaining acquired records are exactly
      | API | partition | error | acknowledgement error | states |
      | ShareFetch | 0 | 0 | 42 | acquired,acquired,acquired |
      | ShareAcknowledge | 0 | 42 | 0 | acquired,acquired,acquired |

  Scenario: Metadata validation rejects a missing partition
    Given reader acquired offsets 0 through 2 of ack-order partition 0 in separate fetch and acknowledge groups
    When principal reader sends these acknowledgement batches for partition 3 through both share APIs
      | first | last | types |
      | 0 | 0 | 1 |
    Then the share API rows and remaining acquired records are exactly
      | API | partition | error | acknowledgement error | states |
      | ShareFetch | 3 | 3 | 3 | acquired,acquired,acquired |
      | ShareAcknowledge | 3 | 3 | 0 | acquired,acquired,acquired |
