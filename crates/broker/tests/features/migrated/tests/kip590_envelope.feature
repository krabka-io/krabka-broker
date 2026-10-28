Feature: Controller envelopes preserve embedded client identity protocol and authorization
  Scenario: A forwarded admin write returns client framed response bytes and reaches metadata
    Given the controller uses the compatibility authorizer on its plaintext listener
    And the forwarded User principal is alice with delegation token flag false
    When CreateTopics for topic envelope-forwarded-topic is forwarded in an envelope
    Then the embedded header is hex 5a5a123400 and the fully consumed CreateTopics response contains
      | topic | code | partitions | replication |
      | envelope-forwarded-topic | 0 | 1 | 1 |
    And controller metadata contains topic envelope-forwarded-topic with 1 partitions

  Scenario: Outer refusals distinguish absent principals forbidden APIs and truncated headers
    Given the controller uses the compatibility authorizer on its plaintext listener
    And the forwarded request has no serialized principal
    When CreateTopics for topic envelope-no-principal is forwarded in an envelope
    Then the envelope returns error code 97 with response data absent
    Given the forwarded User principal is alice with delegation token flag false
    When a default Produce request is forwarded despite not being a forwardable API
    Then the envelope returns error code 42 with response data absent
    When the embedded raw request header bytes hex 001300 are forwarded
    Then the envelope returns error code 35 with response data absent

  Scenario: Envelope discovery is restricted to the controller listener
    Given the controller uses the compatibility authorizer on its plaintext listener
    When API versions are requested from both controller and client listeners
    Then Envelope is advertised only on the controller at versions zero through zero

  Scenario: Host ACLs evaluate the embedded client address rather than the forwarding connection
    Given the plaintext forwarding peer ANONYMOUS is a superuser while embedded principals use ACLs
    And cluster grants authorize these principal host and operation combinations
      | principal | host | operation |
      | User:alice | 10.1.2.3 | Create |
      | User:bob | 127.0.0.1 | Create |
    And the forwarded client address contains octets 10,1,2,3
    When these embedded principals forward CreateTopics using the configured client address
      | principal | topic |
      | alice | envelope-host-acl-alice |
      | bob | envelope-host-acl-bob |
    Then the host authorized CreateTopics responses contain exactly
      | topic | code |
      | envelope-host-acl-alice | 0 |
      | envelope-host-acl-bob | 29 |

  Scenario: Malformed client address octets refuse the envelope before dispatch
    Given the controller uses the compatibility authorizer on its plaintext listener
    And the forwarded User principal is alice with delegation token flag false
    And the forwarded client address contains octets 10,1,2
    When CreateTopics for topic envelope-bad-client-host is forwarded in an envelope
    Then topic envelope-bad-client-host remains absent from the controller metadata after refusal code 42

  Scenario: Producer ID allocation checks the embedded principal without consuming denied blocks
    Given the plaintext forwarding peer ANONYMOUS is a superuser while embedded principals use ACLs
    And cluster grants authorize these principal host and operation combinations
      | principal | host | operation |
      | User:alice | * | ClusterAction |
    When these embedded principals request a producer ID block for the live broker in order
      | principal |
      | bob |
      | alice |
    Then producer ID replies echo header hex 5a5a123400 and contain these complete response fields
      | principal | code | start | length |
      | bob | 31 | 0 | 0 |
      | alice | 0 | 0 | 1000 |

  Scenario: An unauthorized forwarding peer cannot impersonate a client
    Given the controller ACL authorizer has no superusers or forwarding peer grants
    And the forwarded User principal is alice with delegation token flag false
    When CreateTopics for topic envelope-no-cluster-action is forwarded in an envelope
    Then topic envelope-no-cluster-action remains absent from the controller metadata after refusal code 31

  Scenario: The embedded API version is rechecked before dispatch
    Given the controller uses the compatibility authorizer on its plaintext listener
    And the forwarded User principal is alice with delegation token flag false
    When CreateTopics for topic envelope-unsupported-version advertises one version above the latest supported version
    Then topic envelope-unsupported-version remains absent from the controller metadata after refusal code 35

  Scenario: Malformed embedded requests return errors while the controller connection remains usable
    Given the controller uses the compatibility authorizer on its plaintext listener
    And the forwarded User principal is alice with delegation token flag false
    And one controller TCP connection is reused for every forwarded request
    When the latest CreateTopics header forwards this undecodable UTF8 body
      """
      not a CreateTopics body
      """
    Then the envelope returns error code 35 with response data absent
    When the valid CreateTopics frame for envelope-after-refusals is forwarded with 2 trailing bytes removed
    Then the envelope returns error code 35 with response data absent
    When embedded API key 93 version 0 with an empty body is forwarded
    Then the envelope returns error code 35 with response data absent
    When embedded API key 5 version 0 with an empty body is forwarded
    Then the envelope returns error code 35 with response data absent
    When embedded API key 0 version 9 with an empty body is forwarded
    Then the envelope returns error code 42 with response data absent
    When CreateTopics for topic envelope-after-refusals is forwarded in an envelope
    Then the envelope returns error code 0 with response data present
    And controller metadata contains topic envelope-after-refusals with 1 partitions

  Scenario Outline: Unstable API configuration controls forwarding of a trunk API
    Given unstable API versions are enabled <enabled>
    And the controller uses the compatibility authorizer on its plaintext listener
    And the forwarded User principal is alice with delegation token flag false
    When UnregisterController for controller nine is forwarded at its latest version
    Then the envelope returns error code <code> with response data <presence>
    Examples:
      | enabled | code | presence |
      | false | 35 | absent |
      | true | 0 | present |

  Scenario: Delegation token minting follows the embedded client token authentication flag
    Given a SASL PLAIN forwarding connection authenticates user forwarder password forwarder-secret with delegation master key envelope-master-key
    And the forwarded User principal is alice with delegation token flag true
    When the embedded client requests a delegation token without an explicit owner
    Then the whole refused delegation token response has code 64 and identifies User alice as owner and requester with timestamps minus one and empty token fields
    Given the forwarded User principal is alice with delegation token flag false
    When the embedded client requests a delegation token without an explicit owner
    Then the minted delegation token succeeds for User alice and carries a nonempty HMAC
