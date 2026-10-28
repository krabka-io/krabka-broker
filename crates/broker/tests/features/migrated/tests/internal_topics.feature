Feature: Internal topic visibility for administrative clients and pattern subscriptions
  Only broker-owned topic names are internal. A double-underscore prefix alone
  does not classify a topic, and the audit classification follows its configured name.

  Scenario: Metadata classifies broker-owned topics without relying on their prefix alone
    Given a broker hosts these topic names
      | topic |
      | __consumer_offsets |
      | __transaction_state |
      | __share_group_state |
      | __krabka_audit |
      | __barrier_state |
      | __diskless_wal_index |
      | __remote_log_metadata |
      | __user_topic |
      | orders |
    When all topic metadata is requested
    Then metadata reports these topic classifications
      | topic | is_internal |
      | __consumer_offsets | true |
      | __transaction_state | true |
      | __share_group_state | true |
      | __krabka_audit | true |
      | __barrier_state | true |
      | __diskless_wal_index | true |
      | __remote_log_metadata | false |
      | __user_topic | false |
      | orders | false |

  Scenario: Partition descriptions preserve the same topic classification and identity
    Given a broker hosts these topic names
      | topic |
      | __consumer_offsets |
      | __transaction_state |
      | __share_group_state |
      | __krabka_audit |
      | __barrier_state |
      | __diskless_wal_index |
      | __remote_log_metadata |
      | __user_topic |
      | orders |
    When all topic metadata and partition descriptions are requested
    Then partition descriptions agree with metadata on these classifications
      | topic | is_internal |
      | __consumer_offsets | true |
      | __transaction_state | true |
      | __share_group_state | true |
      | __krabka_audit | true |
      | __barrier_state | true |
      | __diskless_wal_index | true |
      | __remote_log_metadata | false |
      | __user_topic | false |
      | orders | false |

  Scenario: Renaming the audit topic moves its internal classification to the configured name
    Given a broker configuration names its audit topic __house_audit
    When the configured audit broker starts
    And all topic metadata is requested
    Then the renamed audit topic is internal and the default audit name is not

  Scenario: An audit topic outside the internal naming convention prevents startup
    Given a broker configuration names its audit topic house_audit
    When the configured audit broker starts
    Then startup refuses the invalid audit_topic configuration
