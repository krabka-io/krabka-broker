Feature: Authorizing and committing SCRAM credential upsertions

  Scenario: alter scram creds super user can provision
    Given a PLAIN admin superuser on a broker supporting SCRAM-SHA-512
    And alice requests a SCRAM-SHA-512 credential derived from her password
    When the PLAIN principal submits alice's AlterUserScramCredentials upsertion
    Then alice is the single successful credential result
    When alice authenticates against the committed credential and requests Metadata
    Then the upserted SCRAM credential and its server signature authenticate successfully

  Scenario: alter scram creds super user can provision sha256
    Given a PLAIN admin superuser on a broker supporting SCRAM-SHA-256
    And alice requests a SCRAM-SHA-256 credential derived from her password
    When the PLAIN principal submits alice's AlterUserScramCredentials upsertion
    Then alice is the single successful credential result
    When alice authenticates against the committed credential and requests Metadata
    Then the upserted SCRAM credential and its server signature authenticate successfully

  Scenario: alter scram creds non super user rejected
    Given PLAIN user bob has no cluster-Alter permission
    And alice requests a SCRAM-SHA-512 credential derived from her password
    When the PLAIN principal submits alice's AlterUserScramCredentials upsertion
    Then the single upsertion result is CLUSTER_AUTHORIZATION_FAILED
