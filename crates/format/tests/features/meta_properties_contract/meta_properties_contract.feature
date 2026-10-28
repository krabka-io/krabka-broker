Feature: Kafka meta.properties file compatibility
  Scenario: A formatted directory writes Kafka's ordered property file
    Given cluster AQIDBAUGBwgJCgsMDQ4PEA node 1 and directory ID U-hCdTzaSPyFB3swwc7Qdw
    And meta.properties is written at Unix second 1709210096
    When the Kafka meta.properties file text is generated
    Then the meta.properties file is exactly
      """
      #
      #Thu Feb 29 12:34:56 UTC 2024
      cluster.id=AQIDBAUGBwgJCgsMDQ4PEA
      directory.id=U-hCdTzaSPyFB3swwc7Qdw
      node.id=1
      version=1
      """

  Scenario: An absent directory ID does not emit a directory property
    Given cluster AQIDBAUGBwgJCgsMDQ4PEA node 2147483647 and directory ID omitted
    And meta.properties is written at Unix second 1709210096
    When the Kafka meta.properties file text is generated
    Then the meta.properties file is exactly
      """
      #
      #Thu Feb 29 12:34:56 UTC 2024
      cluster.id=AQIDBAUGBwgJCgsMDQ4PEA
      node.id=2147483647
      version=1
      """

  Scenario: A file with bad version retains Kafka diagnostics
    Given meta.properties file contents
      """
      version=one
      """
    When the meta.properties file is parsed
    Then the meta.properties error is Invalid meta.properties version string 'one'

  Scenario: A file with unknown version retains Kafka diagnostics
    Given meta.properties file contents
      """
      version=2
      node.id=1
      cluster.id=AQIDBAUGBwgJCgsMDQ4PEA
      """
    When the meta.properties file is parsed
    Then the meta.properties error is Unknown meta.properties version number 2

  Scenario: A file with unsupported zero retains Kafka diagnostics
    Given meta.properties file contents
      """
      version=0
      broker.id=1
      cluster.id=AQIDBAUGBwgJCgsMDQ4PEA
      """
    When the meta.properties file is parsed
    Then the meta.properties error is Unsupported meta.properties version 0: krabka reads version 1, which a KRaft node writes

  Scenario: A file with missing version retains Kafka diagnostics
    Given meta.properties file contents
      """
      node.id=1
      cluster.id=AQIDBAUGBwgJCgsMDQ4PEA
      """
    When the meta.properties file is parsed
    Then the meta.properties error is Unsupported meta.properties version 0: krabka reads version 1, which a KRaft node writes

  Scenario: A file with old json retains Kafka diagnostics
    Given meta.properties file contents
      """
      {
        "cluster_id": "AQIDBAUGBwgJCgsMDQ4PEA",
        "version": 3
      }
      """
    When the meta.properties file is parsed
    Then the meta.properties error is Unsupported meta.properties version 0: krabka reads version 1, which a KRaft node writes

  Scenario: A file with no node or cluster retains Kafka diagnostics
    Given meta.properties file contents
      """
      version=1
      """
    When the meta.properties file is parsed
    Then the meta.properties error is Failed to find node.id

  Scenario: A file with node trailing space retains Kafka diagnostics
    Given a meta.properties file with a trailing space after node ID 1
    When the meta.properties file is parsed
    Then the meta.properties error is Unable to read node.id as a base-10 number.

  Scenario: A file with node overflow retains Kafka diagnostics
    Given meta.properties file contents
      """
      version=1
      node.id=2147483648
      cluster.id=AQIDBAUGBwgJCgsMDQ4PEA
      """
    When the meta.properties file is parsed
    Then the meta.properties error is Unable to read node.id as a base-10 number.

  Scenario: A file with bad directory precedes cluster missing retains Kafka diagnostics
    Given meta.properties file contents
      """
      version=1
      node.id=1
      directory.id=nope
      """
    When the meta.properties file is parsed
    Then the meta.properties error is Unable to read directory.id as a Uuid: Input string `nope` decoded as 3 bytes, which is not equal to the expected 16 bytes of a base64-encoded UUID

  Scenario: A file with no cluster retains Kafka diagnostics
    Given meta.properties file contents
      """
      version=1
      node.id=1
      """
    When the meta.properties file is parsed
    Then the meta.properties error is cluster.id was not found.

  Scenario: A file with invalid cluster retains Kafka diagnostics
    Given meta.properties file contents
      """
      version=1
      node.id=1
      cluster.id=AAAAAA
      """
    When the meta.properties file is parsed
    Then the meta.properties error is Unable to read cluster.id as a Uuid: Input string `AAAAAA` decoded as 4 bytes, which is not equal to the expected 16 bytes of a base64-encoded UUID

  Scenario: A file with malformed escape retains Kafka diagnostics
    Given meta.properties file contents
      """
      version=1
      node.id=1
      cluster.id=\u00zz
      """
    When the meta.properties file is parsed
    Then the meta.properties error is Malformed \uxxxx encoding.

  Scenario: No formatted directory leaves the cluster unknown
    Given directory verification expects cluster discover and node 1
    And formatted directory metadata
      | path | cluster | node | directory |
    When the directory metadata ensemble is verified
    Then the verified directory cluster is unknown

  Scenario: Consistent directories supply the cluster ID
    Given directory verification expects cluster discover and node 1
    And formatted directory metadata
      | path | cluster | node | directory |
      | /a | 193 | 1 | 4096 |
      | /b | 193 | 1 | 8192 |
    When the directory metadata ensemble is verified
    Then the verified directory cluster is 193

  Scenario: Stored directories can supply the node ID
    Given directory verification expects cluster 193 and node discover
    And formatted directory metadata
      | path | cluster | node | directory |
      | /a | 193 | 4 | 4096 |
      | /b | 193 | 4 | 8192 |
    When the directory metadata ensemble is verified
    Then the verified directory cluster is 193

  Scenario: A configured cluster must match stored metadata
    Given directory verification expects cluster 194 and node 1
    And formatted directory metadata
      | path | cluster | node | directory |
      | /a | 193 | 1 | 4096 |
    When the directory metadata ensemble is verified
    Then directory verification refuses a cluster mismatch

  Scenario: A configured node must match stored metadata
    Given directory verification expects cluster discover and node 1
    And formatted directory metadata
      | path | cluster | node | directory |
      | /a | 193 | 2 | 4096 |
    When the directory metadata ensemble is verified
    Then directory verification refuses a configured node mismatch

  Scenario: Every directory must belong to the same node
    Given directory verification expects cluster discover and node discover
    And formatted directory metadata
      | path | cluster | node | directory |
      | /a | 193 | 3 | 4096 |
      | /b | 193 | 4 | 8192 |
    When the directory metadata ensemble is verified
    Then directory verification refuses disagreeing stored node IDs

  Scenario: A reserved directory identity cannot identify a log directory
    Given directory verification expects cluster discover and node 1
    And formatted directory metadata
      | path | cluster | node | directory |
      | /a | 193 | 1 | 2 |
    When the directory metadata ensemble is verified
    Then directory verification refuses a reserved directory ID

  Scenario: Two directories cannot share one directory identity
    Given directory verification expects cluster discover and node 1
    And formatted directory metadata
      | path | cluster | node | directory |
      | /a | 193 | 1 | 4096 |
      | /b | 193 | 1 | 4096 |
    When the directory metadata ensemble is verified
    Then directory verification refuses a duplicate directory ID

  Scenario: Omitting directory IDs creates no duplicate-ID conflict
    Given directory verification expects cluster discover and node 1
    And formatted directory metadata
      | path | cluster | node | directory |
      | /a | 193 | 1 | omitted |
      | /b | 193 | 1 | omitted |
    When the directory metadata ensemble is verified
    Then the verified directory cluster is 193
