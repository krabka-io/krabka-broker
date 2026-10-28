@docker
Feature: Stock share consumers honor group reset policies and expose registered groups
  Scenario: An earliest share group reads every preexisting record
    Given share topic kip932-jvm for group jvm-share-g contains values share-alpha,share-bravo,share-charlie,share-delta
    When the operator sets share reset strategy earliest and a stock share consumer reads 4 records
    Then the share consumer delivers these values
      | value |
      | share-alpha |
      | share-bravo |
      | share-charlie |
      | share-delta |

  Scenario: Stock share administration describes a registered group
    Given share group jvm-share-gd has registered on topic kip932-jvm-d after a stock consumer reads values d-one,d-two
    When the stock share group tool describes the group's coordinator and state
    Then the administrative report contains the registered share group identity

  Scenario: Stock share administration lists a registered group
    Given share group jvm-share-gl has registered on topic kip932-jvm-l after a stock consumer reads values l-one,l-two
    When the stock share group tool lists all share groups
    Then the administrative report contains the registered share group identity

  Scenario: A one hour share reset window skips older batches
    Given share topic kip932-jvm-dur for group jvm-share-gdur contains timestamped batches
      | age_ms | value |
      | 7200000 | dur-stale-one |
      | 7200000 | dur-stale-two |
      | 0 | dur-recent-one |
      | 0 | dur-recent-two |
    When the operator sets share reset strategy by_duration:PT1H and a stock share consumer reads 2 records
    Then the share consumer delivers these values
      | value |
      | dur-recent-one |
      | dur-recent-two |
    And the share consumer withholds these values
      | value |
      | dur-stale-one |
      | dur-stale-two |

  Scenario: A default latest share group joins without reading old records
    Given share topic kip932-jvm-latest for group jvm-share-glatest contains values latest-alpha,latest-bravo
    When a stock share consumer joins with the unchanged default reset strategy and exits after its idle timeout
    Then the share consumer withholds these values
      | value |
      | latest-alpha |
      | latest-bravo |
    When the stock share group tool describes the group's coordinator and state
    Then the administrative report contains the registered share group identity
