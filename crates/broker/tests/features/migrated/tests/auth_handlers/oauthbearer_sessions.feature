Feature: tests auth handlers oauthbearer sessions

  Scenario: oauthbearer session lifetime ms set from token exp
    Then auth handlers oauthbearer sessions: oauthbearer session lifetime ms set from token exp

  Scenario: oauthbearer session capped by connections max reauth
    Then auth handlers oauthbearer sessions: oauthbearer session capped by connections max reauth

  Scenario: oauthbearer session expires closes connection
    Then auth handlers oauthbearer sessions: oauthbearer session expires closes connection

  Scenario: oauthbearer in band reauth with fresh token resets timer
    Then auth handlers oauthbearer sessions: oauthbearer in band reauth with fresh token resets timer

  Scenario: oauthbearer in band reauth with different principal closes
    Then auth handlers oauthbearer sessions: oauthbearer in band reauth with different principal closes

  Scenario: oauthbearer in band reauth with different mechanism closes
    Then auth handlers oauthbearer sessions: oauthbearer in band reauth with different mechanism closes

  Scenario: plain listener session lifetime ms is zero and no timer
    Then auth handlers oauthbearer sessions: plain listener session lifetime ms is zero and no timer
