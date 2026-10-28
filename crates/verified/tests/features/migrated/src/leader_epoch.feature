Feature: Kafka leader epoch checkpoint lookup
  Checkpoint rows use epoch:start-offset notation. A missing or unplaceable epoch
  answers the undefined pair -1:-1; an older epoch ends where the next begins.

  Scenario: Leader epoch lookup follows exact, missing and gap boundaries (1: boundary empty cache, entries empty)
    Given leader epoch checkpoint entries empty
    When epoch 0 is looked up at log end 9
    Then the epoch lookup answers epoch -1 and end offset -1

  Scenario: Leader epoch lookup follows exact, missing and gap boundaries (2: boundary undefined empty, entries empty)
    Given leader epoch checkpoint entries empty
    When epoch -1 is looked up at log end 9
    Then the epoch lookup answers epoch -1 and end offset -1

  Scenario: Leader epoch lookup follows exact, missing and gap boundaries (3: boundary undefined request, entries 0:0,2:50,5:100)
    Given leader epoch checkpoint entries 0:0,2:50,5:100
    When epoch -1 is looked up at log end 200
    Then the epoch lookup answers epoch -1 and end offset -1

  Scenario: Leader epoch lookup follows exact, missing and gap boundaries (4: boundary latest epoch, entries 0:0,2:50,5:100)
    Given leader epoch checkpoint entries 0:0,2:50,5:100
    When epoch 5 is looked up at log end 200
    Then the epoch lookup answers epoch 5 and end offset 200

  Scenario: Leader epoch lookup follows exact, missing and gap boundaries (5: boundary above latest, entries 0:0,2:50,5:100)
    Given leader epoch checkpoint entries 0:0,2:50,5:100
    When epoch 7 is looked up at log end 200
    Then the epoch lookup answers epoch -1 and end offset -1

  Scenario: Leader epoch lookup follows exact, missing and gap boundaries (6: boundary below first, entries 3:30,4:40)
    Given leader epoch checkpoint entries 3:30,4:40
    When epoch 1 is looked up at log end 200
    Then the epoch lookup answers epoch 1 and end offset 30

  Scenario: Leader epoch lookup follows exact, missing and gap boundaries (7: boundary exact older, entries 0:0,2:50,5:100)
    Given leader epoch checkpoint entries 0:0,2:50,5:100
    When epoch 2 is looked up at log end 200
    Then the epoch lookup answers epoch 2 and end offset 100

  Scenario: Leader epoch lookup follows exact, missing and gap boundaries (8: boundary gap above epoch 2, entries 0:0,2:50,5:100)
    Given leader epoch checkpoint entries 0:0,2:50,5:100
    When epoch 3 is looked up at log end 200
    Then the epoch lookup answers epoch 2 and end offset 100

  Scenario: Leader epoch lookup follows exact, missing and gap boundaries (9: boundary gap below latest, entries 0:0,2:50,5:100)
    Given leader epoch checkpoint entries 0:0,2:50,5:100
    When epoch 4 is looked up at log end 200
    Then the epoch lookup answers epoch 2 and end offset 100

  Scenario: Leader epoch lookup follows exact, missing and gap boundaries (10: boundary gap above first, entries 0:0,2:50,5:100)
    Given leader epoch checkpoint entries 0:0,2:50,5:100
    When epoch 1 is looked up at log end 200
    Then the epoch lookup answers epoch 0 and end offset 50
