Feature: Controller traffic enforces peer authentication and authorization

  Scenario: controller listener sasl plaintext two broker quorum
    Given controller peers use matching SASL PLAIN credentials
    When the two controller listeners start with their configured peer security
    Then both controller peers observe two registered brokers

  Scenario: controller listener oauthbearer two broker quorum
    Given controller peers use the same OAuth bearer identity
    When the two controller listeners start with their configured peer security
    Then both controller peers observe two registered brokers

  Scenario: controller listener sasl plaintext rejects mismatched creds
    Given controller peers accept different SASL PLAIN users and passwords
    When the two controller listeners start with their configured peer security
    And peer registrations are observed after a three-second rejection window
    Then the first controller still observes fewer than two brokers

  Scenario: controller listener sasl denies unauthorized principal
    Given controller peers authenticate with matching credentials but deny CLUSTER_ACTION
    When the two controller listeners start with their configured peer security
    And peer registrations are observed after a three-second rejection window
    Then the first controller still observes fewer than two brokers

  Scenario: controller listener plaintext legacy path unchanged
    Given controller peers use anonymous plaintext listeners
    When the two controller listeners start with their configured peer security
    Then both controller peers observe two registered brokers
