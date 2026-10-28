Feature: JVM pre-vote participation after controller loss
  Scenario: contested election krabka counts jvm prevote
    Given a caught-up static quorum of Krabka voters 1 and 2 and JVM voter 3 with cluster id 4b697039-3936-4350-7245-566f74457374
    When the Krabka leader shuts down and the remaining voters have one minute to elect
    Then the surviving Krabka voter is leader at a higher term and the JVM reports no fatal fault
