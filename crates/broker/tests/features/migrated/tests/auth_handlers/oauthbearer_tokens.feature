Feature: tests auth handlers oauthbearer tokens

  Scenario: sasl oauthbearer happy path
    Then auth handlers oauthbearer tokens: sasl oauthbearer happy path

  Scenario: sasl oauthbearer invalid token two round failure
    Then auth handlers oauthbearer tokens: sasl oauthbearer invalid token two round failure

  Scenario: sasl oauthbearer signed token happy path
    Then auth handlers oauthbearer tokens: sasl oauthbearer signed token happy path

  Scenario: sasl oauthbearer signed token wrong key two round failure
    Then auth handlers oauthbearer tokens: sasl oauthbearer signed token wrong key two round failure
