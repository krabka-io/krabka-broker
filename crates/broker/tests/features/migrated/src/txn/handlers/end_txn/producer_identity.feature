Feature: Server transaction aborts fence once and stamp Kafka's transaction version
  Below TV2 the fence bumps the epoch and records TV0; TV2 keeps the producer's
  previous epoch while using the completion bump exactly once.

  Background:
    Given producer 7 has an ongoing transaction at producer epoch 3

  Scenario Outline: Prepare a server abort under transaction version <level>
    Given the cluster transaction version is <level>
    When the server prepares the transaction abort without allocating a fresh producer identity
    Then the abort succeeds at producer epoch <epoch>, retains previous epoch <previous>, stamps transaction version <stamp>, and completes as producer <producer>

    Examples:
      | level | epoch | previous | stamp | producer |
      | 0 | 4 | -1 | 0 | 7 |
      | 1 | 4 | -1 | 0 | 7 |
      | 2 | 4 | 3 | 2 | 7 |
