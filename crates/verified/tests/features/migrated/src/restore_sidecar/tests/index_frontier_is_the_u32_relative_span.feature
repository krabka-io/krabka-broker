Feature: src restore sidecar tests index frontier is the u32 relative span

  Scenario Outline: transaction index follows kafka marker order
    Then restore sidecar tests index frontier is the u32 relative span: transaction index follows kafka marker order for <example>

    Examples:
      | example                        |
      | one abort                      |
      | sequential aborts              |
      | interleaved abort marker order |
      | transaction spans segment roll |
      | marker order regresses         |
      | repeated marker                |
      | marker before segment          |
      | marker past segment            |
      | start after marker             |
      | negative start                 |
      | negative producer              |
