Feature: Prometheus sidecar evidence preserves alert timing and rule health
  Scenario Outline: tests: a target port comes from the metrics url
    Given the metrics URL is <url>
    When the observer extracts the metrics URL port
    Then the observer reads port <port>

    Examples:
      | example  | url                               | port  |
      | loopback | http://127.0.0.1:40123/metrics    | 40123 |
      | no path  | http://host.docker.internal:9404 | 9404  |

  Scenario: A metrics URL without a port cannot be used
    Given the metrics URL is http://127.0.0.1/metrics
    When the observer extracts the metrics URL port
    Then the observer reports that no port can be read

  Scenario: tests: the config scrapes every node as the job the rules name
    Given broker nodes one and two expose metrics on ports 40001 and 40002
    When the observer renders the Prometheus scrape configuration
    Then the canonical Prometheus configuration is:
      """
      global:
        scrape_interval: 5s
        scrape_timeout: 4s
        evaluation_interval: 5s
      rule_files:
        - /etc/prometheus/soak/alert-rules.yaml
      scrape_configs:
        - job_name: krabka-broker
          static_configs:
            - targets: ["host.docker.internal:40001"]
              labels:
                node: "1"
            - targets: ["host.docker.internal:40002"]
              labels:
                node: "2"
      """

  Scenario: Docker publishes one IPv4 API binding
    Given Docker port output is:
      """
      127.0.0.1:32768
      """
    When the observer reads Docker's published API port
    Then the observer reads port 32768

  Scenario: Docker publishes both address families for the same API port
    Given Docker port output is:
      """
      0.0.0.0:32769
      [::]:32769
      """
    When the observer reads Docker's published API port
    Then the observer reads port 32769

  Scenario: An empty published-port response is rejected
    Given Docker port output is empty
    When the observer reads Docker's published API port
    Then the observer reports that no port can be read

  Scenario Outline: tests: ready is a 200
    Given Prometheus readiness replies with HTTP status <status>
    When the observer judges Prometheus readiness
    Then Prometheus readiness is <ready>

    Examples:
      | example  | status | ready |
      | ready    | 200    | true  |
      | starting | 503    | false |

  Scenario: A closed readiness connection is not ready
    Given a proxy closes the readiness connection without a response
    When the observer judges Prometheus readiness
    Then Prometheus readiness is false

  Scenario Outline: tests: milliseconds go out as seconds
    Given an alert timestamp is <millis> Unix milliseconds
    When the observer encodes the timestamp as API seconds and decodes it back
    Then the API timestamp is <seconds> seconds and its millisecond round trip is exact

    Examples:
      | example | millis        | seconds        |
      | whole   | 1700000000000 | 1700000000.000 |
      | millis  | 1700000000042 | 1700000000.042 |

  Scenario Outline: tests: seconds come back as milliseconds
    Given the API returns timestamp <seconds> in seconds
    When the observer decodes API seconds as Unix milliseconds
    Then the decoded millisecond timestamp is <millis>

    Examples:
      | example     | seconds          | millis        |
      | integer     | 1700000000       | 1700000000000 |
      | one digit   | 1700000000.5     | 1700000000500 |
      | past millis | 1700000000.12345 | 1700000000123 |
      | exponent    | 1.7e9            | absent        |

  Scenario: A short history fits in one Prometheus query
    Given the alert query covers zero through 50000 milliseconds at five-second steps
    When the observer splits the query at Prometheus's point limit
    Then the complete query ranges are:
      | start | end   |
      | 0     | 50000 |

  Scenario: A single-point history still issues a query
    Given the alert query covers zero through 0 milliseconds at five-second steps
    When the observer splits the query at Prometheus's point limit
    Then the complete query ranges are:
      | start | end |
      | 0     | 0   |

  Scenario: A long history splits at the ten-thousand-point limit
    Given the alert query covers zero through 75000000 milliseconds at five-second steps
    When the observer splits the query at Prometheus's point limit
    Then the complete query ranges are:
      | start    | end      |
      | 0        | 49995000 |
      | 50000000 | 75000000 |

  Scenario: Pending firing and resolution produce distinct alert intervals
    Given Prometheus returns these alert samples:
      | alert            | state   | seconds     |
      | KrabkaBrokerDown | pending | 100,105,110 |
      | KrabkaBrokerDown | firing  | 115,120     |
    And the alert history ends at second 200
    When the observer folds all returned matrices into alert intervals
    Then KrabkaBrokerDown retains its critical labels and these intervals:
      | state   | from | to  |
      | pending | 100  | 115 |
      | firing  | 115  | 125 |

  Scenario: An alert still firing at the last query step stays open
    Given Prometheus returns these alert samples:
      | alert            | state  | seconds     |
      | KrabkaBrokerDown | firing | 190,195,200 |
    And the alert history ends at second 202
    When the observer folds all returned matrices into alert intervals
    Then KrabkaBrokerDown retains its critical labels and these intervals:
      | state  | from | to   |
      | firing | 190  | open |

  Scenario: A gap in alert samples creates separate pending intervals
    Given Prometheus returns these alert samples:
      | alert            | state   | seconds  |
      | KrabkaBrokerDown | pending | 10,15,40 |
    And the alert history ends at second 200
    When the observer folds all returned matrices into alert intervals
    Then KrabkaBrokerDown retains its critical labels and these intervals:
      | state   | from | to |
      | pending | 10   | 20 |
      | pending | 40   | 45 |

  Scenario: tests: ranges of one series join into one interval
    Given Prometheus returns these alert samples:
      | alert            | state  | seconds |
      | KrabkaBrokerDown | firing | 100,105 |
      | KrabkaBrokerDown | firing | 110     |
    And the alert history ends at second 200
    When the observer folds all returned matrices into alert intervals
    Then KrabkaBrokerDown retains its critical labels and these intervals:
      | state  | from | to  |
      | firing | 100  | 115 |

  Scenario: A Prometheus API error is not an empty alert history
    Given the alert API response is:
      """
      {"status":"error","errorType":"bad_data","error":"exceeded maximum resolution"}
      """
    When the observer parses the response as an alert matrix
    Then the alert matrix response is refused as an error

  Scenario: An instant vector cannot replace an alert matrix
    Given the alert API response is:
      """
      {"status":"success","data":{"resultType":"vector","result":[]}}
      """
    When the observer parses the response as an alert matrix
    Then the alert matrix response is refused as an error

  Scenario: An unrelated JSON answer is rejected
    Given the alert API response is:
      """
      {"nothing":true}
      """
    When the observer parses the response as an alert matrix
    Then the alert matrix response is refused as an error

  Scenario: tests: only alerting rules are recorded with their health
    Given the loaded-rules API response is:
      """
      {"status":"success","data":{"groups":[{"name":"krabka-broker-cluster","file":"/etc/prometheus/soak/alert-rules.yaml","rules":[{"name":"KrabkaBrokerDown","type":"alerting","health":"ok","lastError":""},{"name":"KrabkaBad","type":"alerting","health":"err","lastError":"many-to-many matching not allowed"},{"name":"krabka:rate","type":"recording","health":"ok"}]}]}}
      """
    When the observer reads the health of loaded alerting rules
    Then only BrokerDown and Bad alert rules retain their health and last error

  Scenario: tests: the lines name unhealthy rules and alerts that fired
    Given two alert rules include one unhealthy rule and BrokerDown fires twice while MetadataLag only stays pending
    When the observer renders the final alert history report
    Then the complete alert history report is:
      """
      alert rules: 2 loaded, 1 not ok
      rule KrabkaBad (group g) health err: boom
      KrabkaBrokerDown instance=host.docker.internal:40002 node=2 severity=critical: first fired at +3m00s, still firing at the end (+2h00m00s), firing 1h02m00s in 2 interval(s)
      """

  Scenario Outline: tests: a span reads in hours minutes and seconds
    Given an alert timestamp is <millis> Unix milliseconds
    When the observer renders the duration in hours minutes and seconds
    Then the displayed duration is <text>

    Examples:
      | example | millis    | text       |
      | seconds | 42000     | 42s        |
      | minutes | 125000    | 2m05s      |
      | hours   | 259261000 | 72h01m01s  |
