Feature: Soak host resources and evidence destinations are read correctly
  Scenario Outline: tests: the evidence goes where the runner asks
    Given explicit evidence output is <explicit> and Bazel output is <bazel>
    When the runner chooses its evidence directory
    Then the chosen evidence directory is <chosen>

    Examples:
      | example                           | explicit | bazel | chosen |
      | explicit wins                     | /e       | /b    | /e     |
      | bazel outputs                     | unset    | /b    | /b     |
      | neither                           | unset    | unset | unset  |
      | empty is unset                    | empty    | unset | unset  |
      | empty does not hide bazel outputs  | empty    | /b    | /b     |
      | empty bazel outputs is unset       | unset    | empty | unset  |

  Scenario: Host total memory is expressed in kibibytes
    Given the host memory report is:
      """
      MemTotal:       16384 kB
      MemFree: 1 kB
      """
    When the runner reads host total memory in bytes
    Then the host byte reading is 16777216

  Scenario: Free memory cannot stand in for an absent total
    Given the host memory report is:
      """
      MemFree: 1 kB
      """
    When the runner reads host total memory in bytes
    Then the host byte reading is absent

  Scenario: An unsupported host memory unit is rejected
    Given the host memory report is:
      """
      MemTotal: 16 MB
      """
    When the runner reads host total memory in bytes
    Then the host byte reading is absent

  Scenario: Available disk bytes come from the row below the df header
    Given the df availability report is:
      """
         Avail
      25769803776
      """
    When the runner reads the free bytes below the df header
    Then the host byte reading is 25769803776

  Scenario: A df header alone supplies no free-space reading
    Given the df availability report is:
      """
      Avail
      """
    When the runner reads the free bytes below the df header
    Then the host byte reading is absent

  Scenario: Nonnumeric df data supplies no free-space reading
    Given the df availability report is:
      """
      Avail
      lots
      """
    When the runner reads the free bytes below the df header
    Then the host byte reading is absent
