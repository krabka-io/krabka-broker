Feature: Share group descriptions combine ACL visibility and group state

  Scenario: Group ACLs, topic ACLs and missing groups preserve Kafka response rows
    Given alice may Describe groups g, h and missing, and topic t with UUID 1, but not secret with UUID 2
    And stable share groups at epoch 2 have member m1 at epoch 2, client client at /127.0.0.1 subscribed to t, with unknown UUID eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee and these partition-zero assignments
      | group | topic UUIDs |
      | g | 1 |
      | h | 1,2 |
    When alice describes share groups g,denied,h,missing including authorized operations
    Then the complete ordered share group descriptions are
      | group | code | message | state | epoch | assignment epoch | assignor | authorized operations | assigned topic |
      | denied | 30 | null | | | | | | |
      | g | 0 | null | Stable | 2 | 2 | simple | 256 | t |
      | h | 29 | The group has described topic(s) that the client is not authorized to describe. | | | | | | |
      | missing | 69 | Group missing not found. | | | | | | |
