Feature: Backup client authentication configuration
  Scenario Outline: Cluster operations accept Kafka client properties
    Given a <operation> command targeting broker-1:9093
    And Kafka client properties at /etc/krabka/backup-client.properties
    And a local archive at /mnt/backups
    When the backup command line is parsed
    Then the requested backup operation is selected

    Examples:
      | operation       |
      | capture         |
      | restore-offsets |
