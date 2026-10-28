Feature: tests unsupported feature level

  Scenario: a log finalized at an unstable level starts only with the flag on
    Then unsupported feature level: a log finalized at an unstable level starts only with the flag on

  Scenario Outline: a level above the recovered high watermark is refused before any write
    Then unsupported feature level: a level above the recovered high watermark is refused before any write for <example>

    Examples:
      | example                   |
      | at log start              |
      | missing                   |
      | below a superseding level |

  Scenario: a controller stops when it replays a level it does not support
    Then unsupported feature level: a controller stops when it replays a level it does not support

  Scenario: a controller that stops for any other reason reports no fault
    Then unsupported feature level: a controller that stops for any other reason reports no fault
