Feature: Per-user SCRAM credential mutation validation
  # Kafka errors: 33 UNSUPPORTED_SASL_MECHANISM, 91 RESOURCE_NOT_FOUND,
  # 92 DUPLICATE_RESOURCE, 93 UNACCEPTABLE_CREDENTIAL; 0 is success.

  Scenario: alter scram creds low iterations rejected
    Given a PLAIN-authenticated SCRAM superuser
    And alice requests a valid SHA-512 salted password with only 1 iteration
    When the superuser submits AlterUserScramCredentials
    Then the response has exactly these per-user results:
      | user     | error_code |
      | alice    | 93         |

  Scenario: alter scram creds high iterations rejected but max allowed
    Given a PLAIN-authenticated SCRAM superuser
    And users too-high and max request 16385 and 16384 SHA-512 iterations
    When the superuser submits AlterUserScramCredentials
    Then the response has exactly these per-user results:
      | user     | error_code |
      | too-high | 93         |
      | max      | 0          |

  Scenario: alter scram creds unknown mechanism returns unsupported sasl mechanism
    Given a PLAIN-authenticated SCRAM superuser with a timestamp password
    And alice requests the unrecognized SCRAM mechanism byte 99
    When the superuser submits AlterUserScramCredentials
    Then the response has exactly these per-user results:
      | user     | error_code |
      | alice    | 33         |

  Scenario: alter scram creds duplicate resource rejected
    Given a PLAIN-authenticated SCRAM superuser
    And alice has SHA-512 and SHA-256 upsertions in the same request
    When the superuser submits AlterUserScramCredentials
    Then the response has exactly these per-user results:
      | user     | error_code |
      | alice    | 92         |

  Scenario: alter scram creds duplicate deletion and upsertion rejected per user
    Given a PLAIN-authenticated SCRAM superuser with a unique password
    And alice already has a SHA-512 credential
    And alice has a SHA-512 deletion and a SHA-256 upsertion in the same request
    When the superuser submits AlterUserScramCredentials
    Then the response has exactly these per-user results:
      | user     | error_code |
      | alice    | 92         |

  Scenario: alter scram creds missing deletion returns resource not found 91
    Given a PLAIN-authenticated SCRAM superuser with a unique password
    And the request deletes the nonexistent SHA-512 credential for ghost
    When the superuser submits AlterUserScramCredentials
    Then the response has exactly these per-user results:
      | user     | error_code |
      | ghost    | 91         |
