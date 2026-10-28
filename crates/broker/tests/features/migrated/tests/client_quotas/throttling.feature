Feature: tests client quotas throttling

  Scenario: producer byte rate throttles produce
    Then client quotas throttling: producer byte rate throttles produce

  Scenario: request percentage throttles produce
    Then client quotas throttling: request percentage throttles produce

  Scenario: producer byte rate throttle moves the throttle metrics
    Then client quotas throttling: producer byte rate throttle moves the throttle metrics

  Scenario: consumer byte rate throttles fetch
    Then client quotas throttling: consumer byte rate throttles fetch

  Scenario: user client tuple overrides user specific
    Then client quotas throttling: user client tuple overrides user specific

  Scenario: request percentage throttle is echoed on a patched api
    Then client quotas throttling: request percentage throttle is echoed on a patched api

  Scenario: request percentage throttle is reported on api versions
    Then client quotas throttling: request percentage throttle is reported on api versions
