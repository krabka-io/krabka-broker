Feature: Metadata progress gauges follow Kafka provenance and lag units
  Empty provenance and negative clock lag retain Kafka's observable values.

  Scenario Outline: Sample metadata progress using Kafka units and initial values
    Given metadata progress and sampling time are JSON <progress>
    When the metadata progress gauges are sampled
    Then the complete Kafka metadata gauge sample is JSON <gauges>

    Examples:
      | case | progress | gauges |
      | nothing applied | {"applied":-1,"committed":-1,"append_timestamp":0,"sample_at":1700000000000} | {"applied":-1,"committed":-1,"timestamp":-1,"lag_ms":1700000000001,"lag_records":0} |
      | caught up 250ms after append | {"applied":41,"committed":41,"append_timestamp":1700000000000,"sample_at":1700000000250} | {"applied":41,"committed":41,"timestamp":1700000000000,"lag_ms":250,"lag_records":0} |
      | seven records and a minute behind | {"applied":100,"committed":107,"append_timestamp":1700000000000,"sample_at":1700000060000} | {"applied":100,"committed":107,"timestamp":1700000000000,"lag_ms":60000,"lag_records":7} |
      | source timestamp ahead of local clock | {"applied":5,"committed":5,"append_timestamp":1700000000030,"sample_at":1700000000000} | {"applied":5,"committed":5,"timestamp":1700000000030,"lag_ms":-30,"lag_records":0} |
