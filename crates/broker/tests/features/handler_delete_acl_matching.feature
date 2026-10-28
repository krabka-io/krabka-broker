# Quoted <empty> denotes a zero-length Kafka string; an empty created-topic list contains no names.
Feature: DeleteAcls returns and removes exactly matching bindings

  Background: Mixed literal and prefixed topic permissions
    Given the controller stores these Topic Read ACLs for host wildcard
      | name | pattern | principal | permission |
      | foo | literal | User:alice | allow |
      | * | literal | User:bob | deny |
      | fo | prefixed | User:alice | deny |
      | bar | prefixed | User:bob | allow |
      | food | literal | User:alice | allow |

  Scenario: Empty name and principal match nothing
    When admin deletes Topic ACLs with pattern any, name "<empty>" and principal "<empty>"
    Then the complete DeleteAcls response lists exactly these ACLs and removes only these entries
      | name | pattern | principal | permission |

  Scenario: Empty name matches nothing
    When admin deletes Topic ACLs with pattern any, name "<empty>" and principal "null"
    Then the complete DeleteAcls response lists exactly these ACLs and removes only these entries
      | name | pattern | principal | permission |

  Scenario: Empty principal matches nothing
    When admin deletes Topic ACLs with pattern any, name "null" and principal "<empty>"
    Then the complete DeleteAcls response lists exactly these ACLs and removes only these entries
      | name | pattern | principal | permission |

  Scenario: Null name and principal match every Topic ACL
    When admin deletes Topic ACLs with pattern any, name "null" and principal "null"
    Then the complete DeleteAcls response lists exactly these ACLs and removes only these entries
      | name | pattern | principal | permission |
      | foo | literal | User:alice | allow |
      | * | literal | User:bob | deny |
      | fo | prefixed | User:alice | deny |
      | bar | prefixed | User:bob | allow |
      | food | literal | User:alice | allow |

  Scenario: ANY compares a supplied name exactly
    When admin deletes Topic ACLs with pattern any, name "foo" and principal "null"
    Then the complete DeleteAcls response lists exactly these ACLs and removes only these entries
      | name | pattern | principal | permission |
      | foo | literal | User:alice | allow |

  Scenario: MATCH includes literal wildcard and covering prefixes
    When admin deletes Topic ACLs with pattern match, name "foo" and principal "null"
    Then the complete DeleteAcls response lists exactly these ACLs and removes only these entries
      | name | pattern | principal | permission |
      | foo | literal | User:alice | allow |
      | * | literal | User:bob | deny |
      | fo | prefixed | User:alice | deny |

  Scenario: MATCH still filters by principal
    When admin deletes Topic ACLs with pattern match, name "foo" and principal "User:alice"
    Then the complete DeleteAcls response lists exactly these ACLs and removes only these entries
      | name | pattern | principal | permission |
      | foo | literal | User:alice | allow |
      | fo | prefixed | User:alice | deny |

  Scenario: LITERAL with null name selects all literal ACLs
    When admin deletes Topic ACLs with pattern literal, name "null" and principal "null"
    Then the complete DeleteAcls response lists exactly these ACLs and removes only these entries
      | name | pattern | principal | permission |
      | foo | literal | User:alice | allow |
      | * | literal | User:bob | deny |
      | food | literal | User:alice | allow |

  Scenario: PREFIXED compares the stored prefix exactly
    When admin deletes Topic ACLs with pattern prefixed, name "foo" and principal "null"
    Then the complete DeleteAcls response lists exactly these ACLs and removes only these entries
      | name | pattern | principal | permission |
