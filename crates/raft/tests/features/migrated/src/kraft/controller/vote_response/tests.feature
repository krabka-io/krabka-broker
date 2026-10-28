Feature: Kafka Vote response transitions
  Scenario: A candidate interprets a Vote response against its known epoch and leader (1: known none, leader 2)
    Given node 1 has requested votes at epoch 5 with known leader none
    When a Vote response names leader 2 at epoch 4 with error 0 and grant false
    Then the voter chooses ignore at epoch 0 with leader none and grant false

  Scenario: A candidate interprets a Vote response against its known epoch and leader (2: known none, leader none)
    Given node 1 has requested votes at epoch 5 with known leader none
    When a Vote response names leader none at epoch 5 with error 75 and grant false
    Then the voter chooses ignore at epoch 0 with leader none and grant false

  Scenario: A candidate interprets a Vote response against its known epoch and leader (3: known none, leader 3)
    Given node 1 has requested votes at epoch 5 with known leader none
    When a Vote response names leader 3 at epoch 6 with error 0 and grant false
    Then the voter chooses follow at epoch 6 with leader 3 and grant false

  Scenario: A candidate interprets a Vote response against its known epoch and leader (4: known none, leader 3)
    Given node 1 has requested votes at epoch 5 with known leader none
    When a Vote response names leader 3 at epoch 6 with error 74 and grant false
    Then the voter chooses follow at epoch 6 with leader 3 and grant false

  Scenario: A candidate interprets a Vote response against its known epoch and leader (5: known none, leader none)
    Given node 1 has requested votes at epoch 5 with known leader none
    When a Vote response names leader none at epoch 6 with error 0 and grant false
    Then the voter chooses unattached at epoch 6 with leader none and grant false

  Scenario: A candidate interprets a Vote response against its known epoch and leader (6: known none, leader 1)
    Given node 1 has requested votes at epoch 5 with known leader none
    When a Vote response names leader 1 at epoch 6 with error 0 and grant false
    Then the voter chooses ignore at epoch 0 with leader none and grant false

  Scenario: A candidate interprets a Vote response against its known epoch and leader (7: known none, leader 3)
    Given node 1 has requested votes at epoch 5 with known leader none
    When a Vote response names leader 3 at epoch 5 with error 0 and grant false
    Then the voter chooses follow at epoch 5 with leader 3 and grant false

  Scenario: A candidate interprets a Vote response against its known epoch and leader (8: known none, leader 3)
    Given node 1 has requested votes at epoch 5 with known leader none
    When a Vote response names leader 3 at epoch 5 with error 6 and grant false
    Then the voter chooses follow at epoch 5 with leader 3 and grant false

  Scenario: A candidate interprets a Vote response against its known epoch and leader (9: known 2, leader 3)
    Given node 1 has requested votes at epoch 5 with known leader 2
    When a Vote response names leader 3 at epoch 5 with error 0 and grant false
    Then the voter chooses count at epoch 5 with leader none and grant false

  Scenario: A candidate interprets a Vote response against its known epoch and leader (10: known none, leader none)
    Given node 1 has requested votes at epoch 5 with known leader none
    When a Vote response names leader none at epoch 5 with error 0 and grant true
    Then the voter chooses count at epoch 5 with leader none and grant true

  Scenario: A candidate interprets a Vote response against its known epoch and leader (11: known none, leader none)
    Given node 1 has requested votes at epoch 5 with known leader none
    When a Vote response names leader none at epoch 5 with error 0 and grant false
    Then the voter chooses count at epoch 5 with leader none and grant false

  Scenario: A candidate interprets a Vote response against its known epoch and leader (12: known none, leader none)
    Given node 1 has requested votes at epoch 5 with known leader none
    When a Vote response names leader none at epoch 5 with error 125 and grant false
    Then the voter chooses ignore at epoch 0 with leader none and grant false
