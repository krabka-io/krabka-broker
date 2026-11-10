Feature: Kafka authorized-operation discovery

  Rule: Each resource type advertises exactly the operations Kafka supports on it

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

  Rule: An operation's bit position is its Kafka operation code

    Scenario Outline: Kafka operation codes determine the advertised permission bits
      Given the authorized operation is <operation>
      When the operation is encoded in the authorized-operations bitfield
      Then the operation contributes mask <mask>

      Examples:
        | case     | operation       | mask |
        | code 3   | Read            | 8    |
        | code 4   | Write           | 16   |
        | code 5   | Create          | 32   |
        | code 6   | Delete          | 64   |
        | code 7   | Alter           | 128  |
        | code 8   | Describe        | 256  |
        | code 9   | ClusterAction   | 512  |
        | code 10  | DescribeConfigs | 1024 |
        | code 11  | AlterConfigs    | 2048 |
        | code 12  | IdempotentWrite | 4096 |

  Rule: An allowed operation also reports every supported operation it implies

    Scenario Outline: A grant reports its operation and what it implies
      Given alice is allowed <granted> on a <resource>
      When the broker reports alice's authorized operations on the <resource>
      Then the authorized operations are exactly <reported>

      Examples:
        | case                                   | resource | granted         | reported                                          |
        | read implies describe                  | topic    | Read            | Read,Describe                                     |
        | write implies describe                 | topic    | Write           | Write,Describe                                    |
        | delete implies describe                | group    | Delete          | Delete,Describe                                   |
        | describe configs implies nothing       | group    | DescribeConfigs | DescribeConfigs                                   |
        | alter configs implies describe configs | group    | AlterConfigs    | AlterConfigs,DescribeConfigs                      |
        | all grants every supported operation   | group    | All             | Read,Describe,Delete,DescribeConfigs,AlterConfigs |

  Rule: A denial refuses only the operation it names, not the ones a grant implies

    Scenario: A Read denial leaves the Describe that a Read grant implies
      Given alice is allowed Read on a topic
      And alice is denied Read on the topic
      When the broker reports alice's authorized operations on the topic
      Then the authorized operations are exactly Describe

  Rule: A super user holds every operation the resource supports

    Scenario Outline: A super user reports every supported operation without an ACL
      Given alice is a super user
      When the broker reports alice's authorized operations on a <resource>
      Then the authorized operations are exactly <reported>

      Examples:
        | case    | resource | reported                                                                         |
        | topic   | topic    | Read,Write,Create,Delete,Alter,Describe,DescribeConfigs,AlterConfigs             |
        | group   | group    | Read,Describe,Delete,DescribeConfigs,AlterConfigs                                |
        | cluster | cluster  | Create,Alter,Describe,ClusterAction,AlterConfigs,DescribeConfigs,IdempotentWrite |

  Rule: A principal with no ACL that is not a super user holds no operation

    Scenario: A principal outside the super-user list with no ACL reports an empty bitfield
      Given bob is a super user
      When the broker reports alice's authorized operations on a topic
      Then no operation is authorized
