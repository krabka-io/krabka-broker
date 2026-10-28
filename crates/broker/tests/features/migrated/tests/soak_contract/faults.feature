Feature: Disk-pressure faults respect host reserves and undo their ballast
  Scenario Outline: tests: a disk fill reaches its watermark or is refused
    Given disk capacity is 2 GiB watermark is 25 percent and host reserve is 4 GiB
    And the broker uses <used> bytes and the host has <host_free> bytes free
    When the executor computes ballast needed to reach the disk watermark
    Then the required ballast is <ballast>

    Examples:
      | example                     | used      | host_free   | ballast   |
      | fills to the watermark      | 104857600 | 10737418240 | 432013312 |
      | already past it             | 629145600 | 10737418240 | 0         |
      | exactly the reserve left    | 0         | 4831838208  | 536870912 |
      | refused below the reserve   | 0         | 4830789632  | refused   |

  Scenario: tests: a refusal says why
    Given disk capacity is 500 GiB watermark is 80 percent and host reserve is 50 GiB
    And the broker uses 0 bytes and the host has 107374182400 bytes free
    When the executor computes ballast needed to reach the disk watermark
    Then the disk-fill refusal identifies the host reserve as its reason

  Scenario: tests: a dropped guard undoes its fault
    Given a fault guard owns an existing ballast file
    When the disk-pressure fault guard is dropped
    Then the ballast file no longer exists

  Scenario: tests: a cleared guard reports its cleanup
    Given a fault guard owns an existing ballast file
    When the disk-pressure fault guard is explicitly cleared
    Then cleanup reports that the ballast was removed
    And the ballast file no longer exists
    When another guard clears the same now-missing ballast path
    Then the second cleanup reports that the ballast was never written
