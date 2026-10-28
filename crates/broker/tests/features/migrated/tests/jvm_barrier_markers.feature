Feature: JVM consumers skip interleaved barrier markers
  Scenario: a jvm consumer reads across barrier markers unchanged
    Given a three-partition topic barrier-invisibility and manual barrier group invisibility-cut on a single broker
    When four records per partition are written, a cut completes, four more records are written, and a second cut completes
    Then both cuts succeed completely and JVM offsets include exactly two marker offsets per partition
      | partition | first | second | end |
      | 0         | 4     | 9      | 10  |
      | 1         | 4     | 9      | 10  |
      | 2         | 4     | 9      | 10  |
    When JVM consumers read eight records per partition using read_uncommitted and read_committed
    Then both isolation levels return only these ordered record values and no markers
      | partition | values                                          |
      | 0         | p0-r0,p0-r1,p0-r2,p0-r3,p0-r4,p0-r5,p0-r6,p0-r7 |
      | 1         | p1-r0,p1-r1,p1-r2,p1-r3,p1-r4,p1-r5,p1-r6,p1-r7 |
      | 2         | p2-r0,p2-r1,p2-r2,p2-r3,p2-r4,p2-r5,p2-r6,p2-r7 |
