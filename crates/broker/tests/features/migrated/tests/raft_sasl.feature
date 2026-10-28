Feature: tests raft sasl

  Scenario: controller listener sasl plaintext two broker quorum
    Then raft sasl: controller listener sasl plaintext two broker quorum

  Scenario: controller listener oauthbearer two broker quorum
    Then raft sasl: controller listener oauthbearer two broker quorum

  Scenario: controller listener sasl plaintext rejects mismatched creds
    Then raft sasl: controller listener sasl plaintext rejects mismatched creds

  Scenario: controller listener sasl denies unauthorized principal
    Then raft sasl: controller listener sasl denies unauthorized principal

  Scenario: controller listener plaintext legacy path unchanged
    Then raft sasl: controller listener plaintext legacy path unchanged
