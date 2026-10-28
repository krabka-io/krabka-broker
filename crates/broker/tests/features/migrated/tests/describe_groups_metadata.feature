Feature: Classic group subscription and assignment bytes survive description

  Scenario: describe groups reports member metadata and protocol name
    Given a ready classic coordinator for group cg-describe-metadata with range subscription hex 000172616e67656d657461dead
    When the consumer requests a member id and rejoins using that id
    Then the first join requires a nonempty member id and the rejoin elects that member with range protocol
    When the leader synchronizes assignment hex 61737369676e2d6279746573 and describes the group
    Then the sole described consumer uses range, metadata hex 000172616e67656d657461dead, and assignment hex 61737369676e2d6279746573

  Scenario: describe groups matches real kafka range subscription
    Given a ready classic coordinator for group cg-describe-real-kafka with range subscription hex 000300000001000174ffffffff00000000ffffffffffff
    When the consumer requests a member id and rejoins using that id
    Then the first join requires a nonempty member id and the rejoin elects that member with range protocol
    When the leader synchronizes assignment hex 000300000001000174000000020000000000000001ffffffff and describes the group
    Then the sole described consumer uses range, metadata hex 000300000001000174ffffffff00000000ffffffffffff, and assignment hex 000300000001000174000000020000000000000001ffffffff
