Feature: tests mtls

  Scenario: mtls principal is cert dn and super user bypass works
    Then mtls: mtls principal is cert dn and super user bypass works

  Scenario: an unmappable certificate dn closes the connection
    Then mtls: an unmappable certificate dn closes the connection

  Scenario: a connection with no certificate is served rather than closed
    Then mtls: a connection with no certificate is served rather than closed
