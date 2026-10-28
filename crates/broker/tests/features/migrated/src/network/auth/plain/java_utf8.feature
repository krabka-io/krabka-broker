Feature: Kafka PLAIN Java UTF-8 credential decoding
  The comparison uses JDK-compatible decoded strings, including replacement of
  malformed username bytes and a complete encoded surrogate in one password.

  Background:
    Given a PLAIN exchange is negotiating without a prior authenticated session

  Scenario: A malformed username authenticates by its Java-decoded value
    Given the PLAIN credential table is:
      | user | password |
      | �eve | secret |
    And the received PLAIN payload has these hexadecimal bytes:
      """
      00 ff 65 76 65 00 73 65 63 72 65 74
      """
    When the broker handles PLAIN authentication without a reauthentication lifetime cap
    Then PLAIN authentication succeeds as decoded principal �eve

  Scenario: An encoded surrogate in a password becomes one replacement character
    Given the PLAIN credential table is:
      | user | password |
      | eve | se�cret |
    And the received PLAIN payload has these hexadecimal bytes:
      """
      00 65 76 65 00 73 65 ed a0 80 63 72 65 74
      """
    When the broker handles PLAIN authentication without a reauthentication lifetime cap
    Then PLAIN authentication succeeds as decoded principal eve

  Scenario: Malformed authorization and authentication IDs compare after Java decoding
    Given the PLAIN credential table is:
      | user | password |
      | �eve | secret |
    And the received PLAIN payload has these hexadecimal bytes:
      """
      c0 65 76 65 00 ff 65 76 65 00 73 65 63 72 65 74
      """
    When the broker handles PLAIN authentication without a reauthentication lifetime cap
    Then PLAIN authentication succeeds as decoded principal �eve
