Feature: Compliance Object Lock protects signed archived bytes from an authorized delete

  Scenario: object lock bucket refuses delete of an archived segment
    Given a compliance-locked MinIO bucket backs a readable WORM broker with a fresh trusted Ed25519 key, conditional PUT and SHA256 checksums
    When the JVM producer writes 200 records to the tiered WORM topic
    Then at least two archived log objects each have their own signed manifest
    When the JVM consumer reads the WORM topic through the readable tier
    Then at least 200 records are returned
    When an operator attempts to delete every version of the first archived segment with the broker's credentials
    Then MinIO refuses the delete and the same segment object remains listed
    When the broker stops and a deep audit verifies every body, chain head and signature against its trusted public key
    Then the archive verifies successfully, is fully attested and covers at least two manifests
