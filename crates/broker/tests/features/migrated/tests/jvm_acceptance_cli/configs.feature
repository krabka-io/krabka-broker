Feature: tests jvm acceptance cli configs

  Scenario: kafka configs alter round trip
    Then jvm acceptance cli configs: kafka configs alter round trip

  Scenario: kafka configs describe all shows effective values and their sources
    Then jvm acceptance cli configs: kafka configs describe all shows effective values and their sources

  Scenario: kafka configs alter round trips every registered topic key
    Then jvm acceptance cli configs: kafka configs alter round trips every registered topic key

  Scenario: kafka topics create accepts the streams internal topic configs
    Then jvm acceptance cli configs: kafka topics create accepts the streams internal topic configs

  Scenario: kafka configs refuses tiered storage on a compacted topic
    Then jvm acceptance cli configs: kafka configs refuses tiered storage on a compacted topic

  Scenario: kafka configs entity types round trip as apache kafka does
    Then jvm acceptance cli configs: kafka configs entity types round trip as apache kafka does
