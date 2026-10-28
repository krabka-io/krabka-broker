Feature: Metadata fetch diagnostic headers use Kafka lowercase hexadecimal

  Scenario: Render zero-prefixed and alphabetic hexadecimal bytes consistently
    Given a metadata fetch diagnostic header has bytes 000aff
    When the observer renders the header for a Kafka diagnostic dump
    Then the diagnostic dump is 000aff
