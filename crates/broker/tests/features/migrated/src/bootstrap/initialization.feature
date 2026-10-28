Feature: Broker startup validates and extends formatted Kafka storage directories

  Background:
    Given a fresh unformatted broker storage root


  Scenario: Initialize a formatted node
    Given the meta log directory contains meta.properties:
      """
      version=1
      cluster.id=AQIDBAUGBwgJCgsMDQ4PEA
      node.id=2
      directory.id=AAAAAAAAAAAAAAAAAAABAA
      """
    Given the data-a log directory contains meta.properties:
      """
      version=1
      cluster.id=AQIDBAUGBwgJCgsMDQ4PEA
      node.id=2
      directory.id=AAAAAAAAAAAAAAAAAAACAA
      """
    And the broker configured cluster UUID 01020304-0506-0708-090a-0b0c0d0e0f10
    When node 2 initializes its metadata and two data log directories
    Then all three directories identify node 2 and the same cluster with distinct directory IDs
    And the existing metadata properties file is preserved byte for byte

  Scenario: Initialize a data directory added after the format
    Given the meta log directory contains meta.properties:
      """
      version=1
      cluster.id=AQIDBAUGBwgJCgsMDQ4PEA
      node.id=2
      directory.id=AAAAAAAAAAAAAAAAAAABAA
      """
    When node 2 initializes its metadata and two data log directories
    Then all three directories identify node 2 and the same cluster with distinct directory IDs
    And the existing metadata properties file is preserved byte for byte

  Scenario: Initialize a file without a directory id
    Given the meta log directory contains meta.properties:
      """
      version=1
      cluster.id=AQIDBAUGBwgJCgsMDQ4PEA
      node.id=2
      """
    Given the data-a log directory contains meta.properties:
      """
      version=1
      cluster.id=AQIDBAUGBwgJCgsMDQ4PEA
      node.id=2
      directory.id=AAAAAAAAAAAAAAAAAAACAA
      """
    When node 2 initializes its metadata and two data log directories
    Then all three directories identify node 2 and the same cluster with distinct directory IDs

  Scenario: Initialize the directory of another node
    Given the meta log directory contains meta.properties:
      """
      version=1
      cluster.id=AQIDBAUGBwgJCgsMDQ4PEA
      node.id=2
      directory.id=AAAAAAAAAAAAAAAAAAABAA
      """
    Given the data-a log directory contains meta.properties:
      """
      version=1
      cluster.id=AQIDBAUGBwgJCgsMDQ4PEA
      node.id=3
      directory.id=AAAAAAAAAAAAAAAAAAADAA
      """
    When node 2 initializes its metadata and two data log directories
    Then log directory initialization fails with startup failed: Stored node id 3 doesn't match previous node id 2 in {data-a}/meta.properties. If you moved your data, make sure your configured node id matches. If you intend to create a new node, you should remove all data in your data directories.

  Scenario: Initialize nothing formatted
    When node 2 initializes its metadata and two data log directories
    Then log directory initialization fails with startup failed: No readable meta.properties files found.

  Scenario: Initialize an unreadable metadata log directory
    Given the meta log directory contains meta.properties:
      """
      version=one
      """
    Given the data-a log directory contains meta.properties:
      """
      version=1
      cluster.id=AQIDBAUGBwgJCgsMDQ4PEA
      node.id=2
      directory.id=AAAAAAAAAAAAAAAAAAACAA
      """
    When node 2 initializes its metadata and two data log directories
    Then log directory initialization fails with startup failed: Encountered I/O error in metadata log directory {meta}. Cannot continue.

  Scenario: Initialize an unformatted metadata log directory
    Given the data-a log directory contains meta.properties:
      """
      version=1
      cluster.id=AQIDBAUGBwgJCgsMDQ4PEA
      node.id=2
      directory.id=AAAAAAAAAAAAAAAAAAACAA
      """
    When node 2 initializes its metadata and two data log directories
    Then log directory initialization fails with startup failed: No `meta.properties` found in {meta} (have you run `krabka-format` to format the directory?)

  Scenario: Initialize another configured cluster id
    Given the meta log directory contains meta.properties:
      """
      version=1
      cluster.id=AQIDBAUGBwgJCgsMDQ4PEA
      node.id=2
      directory.id=AAAAAAAAAAAAAAAAAAABAA
      """
    And the broker configured cluster UUID 00000000-0000-0000-0000-0000000000c2
    When node 2 initializes its metadata and two data log directories
    Then log directory initialization fails with bootstrap file "{meta}/meta.properties": INCONSISTENT_CLUSTER_ID: configured cluster id AAAAAAAAAAAAAAAAAAAAwg does not match AQIDBAUGBwgJCgsMDQ4PEA

  Scenario: Initialize a stale metadata log in a data directory
    Given the meta log directory contains meta.properties:
      """
      version=1
      cluster.id=AQIDBAUGBwgJCgsMDQ4PEA
      node.id=2
      directory.id=AAAAAAAAAAAAAAAAAAABAA
      """
    Given the data-a log directory contains meta.properties:
      """
      version=1
      cluster.id=AQIDBAUGBwgJCgsMDQ4PEA
      node.id=2
      directory.id=AAAAAAAAAAAAAAAAAAACAA
      """
    And the data-a data directory also contains a cluster metadata log
    When node 2 initializes its metadata and two data log directories
    Then log directory initialization fails with startup failed: Found unexpected metadata location in data directory `{data-a}/__cluster_metadata-0` (the configured metadata directory is {meta}).
