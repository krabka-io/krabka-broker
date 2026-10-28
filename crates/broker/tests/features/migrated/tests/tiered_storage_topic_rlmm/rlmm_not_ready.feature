Feature: Remote-log publication fails closed while topic metadata is unavailable

  Scenario: copy task skips tiering while rlmm not ready
    Given the topic RLMM bootstraps from dead port 127.0.0.1:1 and copy ticks every 200 milliseconds
    When 80 records seal several 1-KiB segments while the metadata manager remains unavailable
    Then after two seconds no remote log object has been uploaded
