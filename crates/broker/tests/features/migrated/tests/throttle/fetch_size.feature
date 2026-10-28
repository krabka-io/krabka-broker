Feature: tests throttle fetch size

  Scenario: throttle rate caps fetch response size
    Then throttle fetch size: throttle rate caps fetch response size

  Scenario: unthrottled partition unaffected
    Then throttle fetch size: unthrottled partition unaffected

  Scenario: a list naming only the follower throttles nothing on the leader
    Then throttle fetch size: a list naming only the follower throttles nothing on the leader

  Scenario: a follower in the isr is not throttled
    Then throttle fetch size: a follower in the isr is not throttled
