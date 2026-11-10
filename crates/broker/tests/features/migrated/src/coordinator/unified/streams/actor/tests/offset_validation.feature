Feature: Streams member identity fences offset access

  Rule: A commit or a fetch is fenced by the member's identity and epoch

    Scenario Outline: Validate a streams offset commit
      Given streams offset identity <input> with live member m1 at epoch 5
      When the streams group validates committing in partition 0
      Then the streams offset validation returns Kafka code <code>

      Examples:
        | case                                                 | input                                                                                         | code |
        | an anonymous commit to a group without members       | {"live":false,"member_id":"","instance_id":null,"epoch":-1,"offset_commit_version":9}         | 0    |
        | an anonymous commit to a group with members          | {"live":true,"member_id":"","instance_id":null,"epoch":-1,"offset_commit_version":9}          | 25   |
        | an empty member id at a member epoch                 | {"live":true,"member_id":"","instance_id":null,"epoch":5,"offset_commit_version":9}           | 25   |
        | an anonymous transactional commit                    | {"live":true,"member_id":"","instance_id":null,"epoch":-1,"offset_commit_version":null}       | 0    |
        | a transactional commit naming an unknown instance    | {"live":true,"member_id":"","instance_id":"instance","epoch":-1,"offset_commit_version":null} | 25   |
        | a transactional instance commit to an empty group    | {"live":false,"member_id":"","instance_id":"instance","epoch":-1,"offset_commit_version":null} | 0    |
        | an unknown member                                    | {"live":true,"member_id":"m2","instance_id":null,"epoch":5,"offset_commit_version":9}         | 25   |
        | a member commit below version 9                      | {"live":true,"member_id":"m1","instance_id":null,"epoch":5,"offset_commit_version":8}         | 35   |
        | a transactional commit at the member epoch           | {"live":true,"member_id":"m1","instance_id":null,"epoch":5,"offset_commit_version":null}      | 0    |
        | a commit at the member epoch                         | {"live":true,"member_id":"m1","instance_id":null,"epoch":5,"offset_commit_version":9}         | 0    |
        | a commit at a newer epoch                            | {"live":true,"member_id":"m1","instance_id":null,"epoch":6,"offset_commit_version":9}         | 113  |
        | a commit at an older epoch without a topology        | {"live":true,"member_id":"m1","instance_id":null,"epoch":4,"offset_commit_version":9}         | 113  |

    Scenario Outline: Validate a streams offset fetch
      Given streams offset identity <input> with live member m1 at epoch 5
      When the streams group validates fetching offsets
      Then the streams offset validation returns Kafka code <code>

      Examples:
        | case                                 | input                                                                                     | code |
        | an anonymous fetch                   | {"live":true,"member_id":null,"instance_id":null,"epoch":-1,"offset_commit_version":null} | 0    |
        | a fetch at the member epoch          | {"live":true,"member_id":"m1","instance_id":null,"epoch":5,"offset_commit_version":null}  | 0    |
        | a fetch at an older epoch            | {"live":true,"member_id":"m1","instance_id":null,"epoch":4,"offset_commit_version":null}  | 113  |
        | a fetch at a newer epoch             | {"live":true,"member_id":"m1","instance_id":null,"epoch":6,"offset_commit_version":null}  | 113  |
        | an unknown member                    | {"live":true,"member_id":"ghost","instance_id":null,"epoch":5,"offset_commit_version":null} | 25   |
        | no member id at a member epoch       | {"live":true,"member_id":null,"instance_id":null,"epoch":5,"offset_commit_version":null}  | 25   |

  Rule: A commit at an older epoch is accepted only for tasks the member held at that epoch

    Scenario Outline: An older-epoch commit of tasks the member held at epoch 4 is accepted
      Given member m1 at epoch 5 holds tasks of the source topic named in, assigned from epoch 3 to 5
      When the member commits <partitions> at epoch 4
      Then the commit is accepted

      Examples:
        | case                                    | partitions |
        | no partitions                           | none       |
        | a task assigned at epoch 3              | in-0       |
        | a task assigned at exactly epoch 4      | in-5       |
        | a task pending revocation since epoch 4 | in-2       |
        | a repartition source topic              | rep-0      |

    Scenario Outline: An older-epoch commit of a task the member did not hold at epoch 4 is refused
      Given member m1 at epoch 5 holds tasks of the source topic named in, assigned from epoch 3 to 5
      When the member commits <partitions> at epoch 4
      Then the commit is refused with STALE_MEMBER_EPOCH

      Examples:
        | case                                       | partitions |
        | a task assigned at epoch 5                 | in-1       |
        | a task with no recorded epoch at epoch 5   | in-3       |
        | one late task refuses the whole commit     | in-0,in-1  |
        | a task the member does not hold            | in-4       |
        | a task of another subtopology              | other-0    |
        | a topic outside the topology               | unknown-0  |
