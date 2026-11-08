Feature: Metadata consumer lifecycle
  Scenario: Initial connection retries without reassignment
    Given the initial metadata connection fails once before returning client 42
    When the metadata consumer connects without being reassigned
    Then client 42 is returned after exactly two connection attempts

  Scenario Outline: Cancellation interrupts connection and retry waits
    Given metadata connection retries wait one hour
    When the consumer is cancelled while <stage>
    Then connection completion reports cancellation immediately

    Examples:
      | case                         | stage                                |
      | a pending initial connection | its initial connection is pending    |
      | a pending retry wait         | waiting to retry a failed connection |
