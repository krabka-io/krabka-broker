Feature: Metadata snapshot codec and restart recovery
  Snapshots preserve metadata and KIP-853 quorum controls for controller/observer
  recovery. Archive restoration intentionally excludes those control records.

  @codec
  Scenario: Topic metadata round trips through the public snapshot codec
    Given a metadata image containing topic snapshot-codec
    When the public snapshot codec serializes and decodes the image
    Then the restored topic metadata exactly matches the original image

  @codec
  Scenario: Observer snapshot images preserve quorum controls that archives omit
    Given a metadata image containing topic snapshot-image
    And the image also contains KRaft version 1 and voter 3001 at controller.example port 9592
    When the public snapshot codec serializes and decodes the image
    Then observer recovery retains the quorum controls while archive restore keeps only the topic

  @restart
  Scenario: A manual snapshot survives a full controller restart
    Given a fresh single-voter metadata controller
    And topic t and finalized metadata.version 25 are committed
    When the operator requests a manual metadata snapshot
    Then the committed topic feature level and voter are present and the checkpoint exists on disk
    When the controller stops and rejoins from that same metadata directory
    Then restart recovers topic t metadata.version 25 its finalized epoch and voter 1
