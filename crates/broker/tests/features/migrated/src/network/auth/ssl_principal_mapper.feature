Feature: Kafka TLS subject-to-principal mapping
  Ordered rules, whole-DN matching, capture substitution and DEFAULT fallback
  select the same ACL principal as Kafka's documented SSL principal mapper.

  Scenario Outline: Map Kafka documented TLS subjects
    Given the ordered TLS principal mapping rules are JSON <rules>
    And the TLS peer subject DN is <distinguished_name>
    When the broker maps the TLS subject to its ACL principal
    Then the TLS ACL principal is JSON <expected>

    Examples:
      | case | rules | distinguished_name | expected |
      | A matching service-user rule extracts the common name | ["RULE:^CN=(.*?),OU=ServiceUsers.*$/$1/"] | CN=serviceuser,OU=ServiceUsers,O=Unknown,L=Unknown,ST=Unknown,C=Unknown | "serviceuser" |
      | A nonmatching rule without DEFAULT refuses the subject | ["RULE:^CN=(.*?),OU=ServiceUsers.*$/$1/"] | CN=adminUser,OU=Admin,O=Unknown,L=Unknown,ST=Unknown,C=Unknown | null |
      | A lowercasing rule builds a principal from two DN components | ["RULE:^CN=(.*?),OU=(.*?),O=(.*?),L=(.*?),ST=(.*?),C=(.*?)$/$1@$2/L"] | CN=adminUser,OU=Admin,O=Unknown,L=Unknown,ST=Unknown,C=Unknown | "adminuser@admin" |
      | An uppercasing rule accepts a lowercase DN attribute name | ["RULE:^.*[Cc][Nn]=([a-zA-Z0-9.]*).*$/$1/U"] | cn=User,OU=Admin,O=Unknown,L=Unknown,ST=Unknown,C=Unknown | "USER" |
      | DEFAULT preserves the entire subject DN | ["DEFAULT"] | CN=alice,OU=x,O=y | "CN=alice,OU=x,O=y" |
      | The first matching rule shadows a DEFAULT tail | ["RULE:^CN=(.*?),.*$/$1/","DEFAULT"] | CN=alice,OU=integration,O=krabka | "alice" |
      | DEFAULT preserves an unmatched subject after a mapping rule | ["RULE:^CN=(.*?),.*$/$1/","DEFAULT"] | OU=integration,O=krabka | "OU=integration,O=krabka" |
      | An explicitly empty rule list refuses every subject | [] | CN=alice,OU=x,O=y | null |
