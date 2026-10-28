@docker
Feature: Stock producers expose the freeze scope that refuses a write
  Scenario: Synchronous producers distinguish literal prefix and absent freezes
    Given these existing topics have the following write-freeze policies
      | topic                | pattern  | scope         | reason              |
      | kfc9-orders          | literal  | kfc9-orders   | DR cutover          |
      | kfc9-tenant-a.events | prefixed | kfc9-tenant-a. | tenant offboarding  |
      | kfc9-control         | none     |               |                     |
    When the stock synchronous producer writes value a record to each topic
    Then each producer exits and reports these exact freeze observations
      | topic                | outcome  | message |
      | kfc9-orders          | refused  | a write freeze on the literal scope "kfc9-orders" refuses this write: DR cutover |
      | kfc9-tenant-a.events | refused  | a write freeze on the prefixed scope "kfc9-tenant-a." refuses this write: tenant offboarding |
      | kfc9-control         | accepted | |
