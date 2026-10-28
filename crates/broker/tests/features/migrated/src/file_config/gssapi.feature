Feature: Kerberos configuration defaults to Kafka service identities
  Incoming and outgoing GSSAPI configurations select Kafka's service name
  when the operator omits one.

  Scenario: Incoming Kerberos inherits Kafka service and clock skew defaults
    Given a Kerberos configuration file contains:
      """
      [gssapi]
      keytab_path = "/k/keytab"
      principal_to_local_rules = ["DEFAULT"]
      """
    When the Kerberos file is parsed and applied to a default broker
    Then incoming Kerberos uses service kafka and the default maximum time skew

  Scenario: Outgoing broker Kerberos credentials inherit Kafka service
    Given a Kerberos configuration file contains:
      """
      [inter_broker_credentials]
      type = "gssapi"
      keytab_path = "/k/keytab"
      client_principal = "kafka@EXAMPLE.COM"
      kdc_url = "tcp://kdc:88"
      """
    When the Kerberos file is parsed and applied to a default broker
    Then inter-broker Kerberos credentials use service kafka
