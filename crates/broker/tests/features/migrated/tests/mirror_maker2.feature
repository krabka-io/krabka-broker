Feature: Stock MirrorMaker migrates a Kafka cluster onto krabka

  @docker
  Scenario: Stock MirrorMaker migrates records consumer position and live configuration
    Given a stock Kafka source holds ten keyed and headered records with its consumer group committed at four and krabka is the empty target
    When stock MirrorMaker starts and copies the source topic onto krabka
    Then every mirrored record retains its source key value and headers
    When the source operator changes retention after mirroring is established
    Then the mirror internal topics are single partition compacted topics
    When the consumer group cuts over to krabka and resumes reading
    Then the translated position is four and the consumer receives exactly the unread source suffix
    And the live source retention change reaches the target topic
    When the operator inspects mirror logs and attempts the target side ACL operation
    Then ACL sync logs contain no failure and the unsecured target reports Kafka's security disabled exception
