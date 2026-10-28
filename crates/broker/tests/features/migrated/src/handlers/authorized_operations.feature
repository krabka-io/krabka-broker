Feature: Kafka authorized-operation discovery
  Clients receive the permissions supported by each resource type and use
  Kafka's operation-code bit positions to decode authorization bitfields.

  Scenario: Topics expose exactly their supported permissions
    Given an authorization query concerns a topic resource
    When the broker lists the resource's supported permissions
    Then the supported permissions are exactly Read,Write,Create,Delete,Alter,Describe,DescribeConfigs,AlterConfigs

  Scenario: Groups expose exactly their supported permissions
    Given an authorization query concerns a group resource
    When the broker lists the resource's supported permissions
    Then the supported permissions are exactly Read,Describe,Delete,DescribeConfigs,AlterConfigs

  Scenario: Clusters expose exactly their supported permissions
    Given an authorization query concerns a cluster resource
    When the broker lists the resource's supported permissions
    Then the supported permissions are exactly Create,Alter,Describe,ClusterAction,AlterConfigs,DescribeConfigs,IdempotentWrite

  Scenario Outline: Kafka operation codes determine the advertised permission bits
    Given the authorized operation is <operation>
    When the operation is encoded in the authorized-operations bitfield
    Then the operation contributes mask <mask>

    Examples:
      | operation       | mask |
      | Read            | 8    |
      | Write           | 16   |
      | Create          | 32   |
      | Delete          | 64   |
      | Alter           | 128  |
      | Describe        | 256  |
      | ClusterAction   | 512  |
      | DescribeConfigs | 1024 |
      | AlterConfigs    | 2048 |
      | IdempotentWrite | 4096 |
