Feature: The HTTP metrics endpoint exposes live broker behavior and evicts rejected labels
  Scenario: OpenMetrics exposes initial families and records client traffic
    Given a broker exposes its default metric families before any client traffic
    When the operator scrapes the OpenMetrics endpoint
    Then the initial exposition includes scalar gauges counters replica lag and the OpenMetrics EOF marker
    When the client creates a topic produces one record and fetches it after commitment
    And the operator scrapes the OpenMetrics endpoint
    Then the successful traffic appears in topic counters controller event timings and live controller gauges

  Scenario: Partition traffic and disk usage appear alongside topic metrics
    Given a broker exposes metrics and scans partition disk usage once per second
    When one produced record materializes a local segment and the disk scanner observes its size
    And the operator scrapes the OpenMetrics endpoint
    Then successful production exposes topic and partition bytes CPU time and a disk usage gauge

  Scenario: Rejected topic and partition labels are removed during reconciliation
    Given a broker exposes metrics with client label eviction enabled by its default configuration
    When the client produces to topic invented-topic partition 0
    Then the invalid produce destination is refused
    When creating the valid topic triggers reconciliation and metrics are observed until the invented topic label disappears
    Then the invented topic does not remain in the metrics body
    When the client produces to topic metrics-it partition 99
    Then the invalid produce destination is refused
    When creating another topic triggers reconciliation and metrics are observed until partition ninety nine disappears
    Then the rejected topic partition pair does not remain in the metrics body
