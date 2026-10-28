Feature: Protect metadata across lossy version downgrades

  Scenario: Reject safe and unsafe metadata loss without changing directory metadata
    Given a three-broker mixed cluster at metadata capability 25 with topic krabka-mv-capability-existing
    When Kafka features runs downgrade arguments ["downgrade","--metadata","3.7-IV1"]
    Then the metadata downgrade fails with message Unsupported metadata.version downgrade from 25 to 16. Refusing to perform the requested downgrade because it might delete metadata information.
    When Kafka features runs downgrade arguments ["downgrade","--metadata","3.7-IV1","--unsafe"]
    Then the metadata downgrade fails with message Unsupported metadata.version downgrade from 25 to 16. Unsafe metadata downgrade is not supported in this version.
    And finalized metadata versions, broker log directories and topic directories equal their baseline
    And the metadata downgrade cluster is shut down
