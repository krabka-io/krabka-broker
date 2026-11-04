Feature: DeleteGroups errors respect the request version
  Scenario Outline: legacy_delete_error_mapping
    Given DeleteGroups version <version> with coordinator error <input>
    When the handler renders the group deletion result
    Then the result has error <output> and message <message>

    Examples:
      | version | input | output | message |
      | 0       | 134   | -1     | null    |
      | 1       | 134   | -1     | null    |
      | 2       | 134   | -1     | null    |
      | 3       | 134   | 134    | generic |
      | 2       | 0     | 0      | null    |
      | 3       | 0     | 0      | null    |
      | 2       | 30    | 30     | null    |
      | 3       | 30    | 30     | null    |
      | 2       | 69    | 69     | null    |
      | 3       | 69    | 69     | null    |
      | 2       | 68    | 68     | null    |
      | 3       | 68    | 68     | null    |
