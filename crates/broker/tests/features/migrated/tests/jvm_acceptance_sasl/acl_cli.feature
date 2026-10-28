Feature: Kafka ACL administration and shorthand parity
  Scenario: jvm kafka acls provision via cli
    Given a PLAIN SASL broker authenticates admin as superuser for Kafka ACL provisioning
    When the Kafka 3.5 ACL tool applies these arguments and lists topic foo
      """
      --add --allow-principal User:alice --operation Read --topic foo
      """
    Then the listing contains these ACL fields
      | field      |
      | User:alice |
      | READ       |
      | ALLOW      |
    When the Kafka 3.5 ACL tool applies these arguments and lists topic foo
      """
      --remove --force --allow-principal User:alice --operation Read --topic foo
      """
    Then the listing no longer contains User:alice

  Scenario: acl shorthands expand as apache kafka expands them
    Given Kafka authorizes anonymous administration and Krabka authorizes the PLAIN admin superuser
    When both Kafka ACL clients add these shorthand bindings and list only the named principal
      | principal                           | arguments |
      | User:krabka-acl-producer             | --add --allow-principal User:krabka-acl-producer --producer --topic krabka-acl-shorthand-topic |
      | User:krabka-acl-idempotent-producer  | --add --allow-principal User:krabka-acl-idempotent-producer --producer --idempotent --topic krabka-acl-shorthand-topic |
      | User:krabka-acl-transactional-producer | --add --allow-principal User:krabka-acl-transactional-producer --producer --topic krabka-acl-shorthand-topic --transactional-id krabka-acl-shorthand-txn |
      | User:krabka-acl-consumer             | --add --allow-principal User:krabka-acl-consumer --consumer --topic krabka-acl-shorthand-topic --group krabka-acl-shorthand-group |
      | User:krabka-acl-cluster              | --add --allow-principal User:krabka-acl-cluster --operation ClusterAction --cluster |
      | User:krabka-acl-denied               | --add --deny-principal User:krabka-acl-denied --deny-host 10.11.12.13 --operation Read --topic krabka-acl-shorthand-topic |
    Then both listings contain these required bindings, isolate each principal, and match Kafka exactly
      | principal                            | host        | resource_type    | resource_name                | operation        | permission |
      | User:krabka-acl-producer              | *           | TOPIC            | krabka-acl-shorthand-topic    | WRITE            | ALLOW |
      | User:krabka-acl-producer              | *           | TOPIC            | krabka-acl-shorthand-topic    | DESCRIBE         | ALLOW |
      | User:krabka-acl-idempotent-producer   | *           | TOPIC            | krabka-acl-shorthand-topic    | WRITE            | ALLOW |
      | User:krabka-acl-idempotent-producer   | *           | CLUSTER          | kafka-cluster                | IDEMPOTENT_WRITE | ALLOW |
      | User:krabka-acl-transactional-producer | *           | TOPIC            | krabka-acl-shorthand-topic    | WRITE            | ALLOW |
      | User:krabka-acl-transactional-producer | *           | TRANSACTIONAL_ID | krabka-acl-shorthand-txn      | WRITE            | ALLOW |
      | User:krabka-acl-transactional-producer | *           | TRANSACTIONAL_ID | krabka-acl-shorthand-txn      | DESCRIBE         | ALLOW |
      | User:krabka-acl-consumer              | *           | TOPIC            | krabka-acl-shorthand-topic    | READ             | ALLOW |
      | User:krabka-acl-consumer              | *           | TOPIC            | krabka-acl-shorthand-topic    | DESCRIBE         | ALLOW |
      | User:krabka-acl-consumer              | *           | GROUP            | krabka-acl-shorthand-group    | READ             | ALLOW |
      | User:krabka-acl-cluster               | *           | CLUSTER          | kafka-cluster                | CLUSTER_ACTION   | ALLOW |
      | User:krabka-acl-denied                | 10.11.12.13 | TOPIC            | krabka-acl-shorthand-topic    | READ             | DENY |
    When both clients remove these shorthand arguments and list principal User:krabka-acl-consumer
      """
      --remove --force --allow-principal User:krabka-acl-consumer --consumer --topic krabka-acl-shorthand-topic --group krabka-acl-shorthand-group
      """
    Then both removals and listings succeed and no consumer bindings remain
