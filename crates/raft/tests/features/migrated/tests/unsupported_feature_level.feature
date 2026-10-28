Feature: Unsupported finalized feature levels stop controllers without damaging metadata
  Scenario: a log finalized at an unstable level starts only with the flag on
    Given a stopped controller log finalized at unstable metadata version 33
    When the controller rejoins with unstable features disabled
    Then startup reports the exact unsupported feature fault before changing any protected metadata file
    When the same controller log rejoins with unstable features enabled
    Then the recovered controller publishes finalized metadata version 33
  Scenario Outline: a level above the recovered high watermark is refused before any write
    Given the finalized unstable log has recovered high watermark <watermark>
    When the controller rejoins with unstable features disabled
    Then startup reports the exact unsupported feature fault before changing any protected metadata file
    Examples:
      | watermark |
      | at-log-start |
      | missing |
      | below-superseding-level |
  Scenario: a controller stops when it replays a level it does not support
    Given a running controller with unstable features enabled false
    Then the controller has not reported a fatal fault
    When metadata version 33 commits and subsequent submissions observe the controller stopping
    Then the stopped controller rejects changes with Shutdown and publishes the unsupported feature reason
  Scenario: a controller that stops for any other reason reports no fault
    Given a running controller with unstable features enabled true
    When metadata version 33 commits and the supported controller shuts down normally
    Then the fatal channel closes without ever reporting a fault
