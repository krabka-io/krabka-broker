Feature: tests jbod enospc

  Scenario: prometheus value parser ignores help and other metrics
    Then jbod enospc: prometheus value parser ignores help and other metrics

  Scenario: real enospc moves leadership and preserves acked records across restart
    Then jbod enospc: real enospc moves leadership and preserves acked records across restart
