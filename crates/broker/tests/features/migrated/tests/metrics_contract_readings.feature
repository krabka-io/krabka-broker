Feature: Metric tooling interprets sample names and canonical exposition correctly
  Scenario: Metric types expand to the sample names Prometheus exports
    Given a metric exposition or query has the text
      """
      # HELP krabka_broker_a A counter.
      # TYPE krabka_broker_a counter
      krabka_broker_a_total 1
      # HELP krabka_broker_b A gauge.
      # TYPE krabka_broker_b gauge
      # HELP krabka_broker_c A histogram.
      # TYPE krabka_broker_c histogram
      # EOF
      """
    When the exposition reader expands sample names according to metric type
    Then the extracted metric names are exactly
      | name |
      | krabka_broker_a_total |
      | krabka_broker_b |
      | krabka_broker_c_bucket |
      | krabka_broker_c_sum |
      | krabka_broker_c_count |

  Scenario: Canonical exposition sorts unordered samples without sorting histogram buckets
    Given a metric exposition or query has the text
      """
      # HELP krabka_broker_a A counter.
      # TYPE krabka_broker_a counter
      krabka_broker_a_total{path="vectored"} 0
      krabka_broker_a_total{path="pread"} 0
      # HELP krabka_broker_c A histogram.
      # TYPE krabka_broker_c histogram
      krabka_broker_c_bucket{le="0.5"} 0
      krabka_broker_c_bucket{le="+Inf"} 0
      krabka_broker_c_sum 0.0
      krabka_broker_c_count 0
      # EOF
      """
    When the exposition is canonicalized for a stable snapshot
    Then counter samples sort while histogram buckets retain their encoded order
      """
      # HELP krabka_broker_a A counter.
      # TYPE krabka_broker_a counter
      krabka_broker_a_total{path="pread"} 0
      krabka_broker_a_total{path="vectored"} 0
      # HELP krabka_broker_c A histogram.
      # TYPE krabka_broker_c histogram
      krabka_broker_c_bucket{le="0.5"} 0
      krabka_broker_c_bucket{le="+Inf"} 0
      krabka_broker_c_sum 0.0
      krabka_broker_c_count 0
      # EOF
      """

  Scenario: Query extraction selects metric tokens instead of labels and functions
    Given a metric exposition or query has the text
      """
      sum by (topic) (rate(krabka_broker_topic_bytes_in_total{topic!="__x"}[5m]))
          / histogram_quantile(0.99, rate(krabka_broker_request_duration_seconds_bucket[5m]))
      """
    When the query reader extracts referenced metric tokens
    Then the extracted metric names are exactly
      | name |
      | krabka_broker_topic_bytes_in_total |
      | krabka_broker_request_duration_seconds_bucket |
