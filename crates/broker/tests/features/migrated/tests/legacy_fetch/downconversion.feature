Feature: tests legacy fetch downconversion

  Scenario: fetch v3 downconverts v2 batch to v0 messageset
    Then legacy fetch downconversion: fetch v3 downconverts v2 batch to v0 messageset

  Scenario: fetch v0 downconverts to magic v0 without timestamps
    Then legacy fetch downconversion: fetch v0 downconverts to magic v0 without timestamps

  Scenario: fetch v3 meters every converted partition of a topic
    Then legacy fetch downconversion: fetch v3 meters every converted partition of a topic
