Feature: CIDR host authorization across the broker and gateway

  Rule: CIDR host matching requires metadata.version 4.4-IV1
    Scenario Outline: A range grant takes effect only after its feature level is finalized
      Given the cluster metadata.version is <version>
      And Alice has a READ allow ACL on orders from 10.0.0.0/8
      When Alice requests READ on orders from 10.1.2.3
      Then the broker and gateway decisions are <decision>

      Examples:
        | case                 | version     | decision |
        | no finalized version | unfinalized | denied   |
        | below the CIDR floor | 4.4-IV0     | denied   |
        | at the CIDR floor    | 4.4-IV1     | allowed  |
        | above the CIDR floor | 4.4-IV2     | allowed  |

    Scenario Outline: A range denial takes effect only after its feature level is finalized
      Given the cluster metadata.version is <version>
      And Alice has a READ allow ACL on orders from *
      And Alice has a READ deny ACL on orders from 10.0.0.0/8
      When Alice requests READ on orders from 10.1.2.3
      Then the broker and gateway decisions are <decision>

      Examples:
        | case                 | version | decision |
        | below the CIDR floor | 4.4-IV0 | allowed  |
        | at the CIDR floor    | 4.4-IV1 | denied   |

  Rule: CIDR grants compare canonical peer addresses numerically
    Scenario Outline: IPv4 peers must fall within the granted range
      Given the cluster metadata.version is 4.4-IV1
      And Alice has a READ allow ACL on orders from 10.1.2.0/24
      When Alice requests READ on orders from <peer>
      Then the broker and gateway decisions are <decision>

      Examples:
        | case             | peer            | decision |
        | before the range | 10.1.1.255      | denied   |
        | first address    | 10.1.2.0        | allowed  |
        | last address     | 10.1.2.255      | allowed  |
        | after the range  | 10.1.3.0        | denied   |
        | mapped IPv4 peer | ::ffff:10.1.2.3 | allowed  |
        | IPv6 peer        | 2001:db8::1     | denied   |

    Scenario Outline: IPv6 peers must fall within the granted range
      Given the cluster metadata.version is 4.4-IV1
      And Alice has a READ allow ACL on orders from 2001:db8::/126
      When Alice requests READ on orders from <peer>
      Then the broker and gateway decisions are <decision>

      Examples:
        | case            | peer        | decision |
        | first address   | 2001:db8::  | allowed  |
        | last address    | 2001:db8::3 | allowed  |
        | after the range | 2001:db8::4 | denied   |
        | IPv4 peer       | 10.1.2.3    | denied   |

  Rule: Kafka excludes unspecified IPv4 peers from CIDR grants
    Scenario Outline: Unspecified peers follow the address-family rule
      Given the cluster metadata.version is 4.4-IV1
      And Alice has a READ allow ACL on orders from <range>
      When Alice requests READ on orders from <peer>
      Then the broker and gateway decisions are <decision>

      Examples:
        | case                    | range     | peer           | decision |
        | unspecified IPv4        | 0.0.0.0/0 | 0.0.0.0        | denied   |
        | mapped unspecified IPv4 | 0.0.0.0/0 | ::ffff:0.0.0.0 | denied   |
        | unspecified IPv6        | ::/0      | ::             | allowed  |

  Rule: A matching range denial overrides a wildcard host grant
    Scenario Outline: Only peers in the denied range lose access
      Given the cluster metadata.version is 4.4-IV1
      And Alice has a READ allow ACL on orders from *
      And Alice has a READ deny ACL on orders from 10.1.2.0/24
      When Alice requests READ on orders from <peer>
      Then the broker and gateway decisions are <decision>

      Examples:
        | case             | peer            | decision |
        | inside the range | 10.1.2.3        | denied   |
        | mapped IPv4 peer | ::ffff:10.1.2.3 | denied   |
        | after the range  | 10.1.3.0        | allowed  |

  Rule: Malformed stored ranges grant no access
    Scenario: A stored host has an invalid IPv4 prefix length
      Given the cluster metadata.version is 4.4-IV1
      And Alice has a READ allow ACL on orders from 10.1.2.0/33
      When Alice requests READ on orders from 10.1.2.3
      Then the broker and gateway decisions are denied
