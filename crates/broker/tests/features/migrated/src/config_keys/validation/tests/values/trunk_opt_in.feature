Feature: Trunk topic keys require opt-in on both individual and whole-map paths
  Released mode reports the exact unknown key while unstable mode preserves canonical storage.

  Scenario Outline: Trunk topic keys require opt-in on both individual and whole-map paths
    Given a topic alteration supplies key <key> and quoted value <value>
    When the supplied key is validated individually and as a map in released and unstable modes
    Then released mode refuses the unknown key and unstable mode stores quoted value <canonical> exactly

    Examples:
      | key | value | canonical |
      | remote.copy.lag.ms | " 1000 " | "1000" |
      | remote.copy.lag.bytes | "-1" | "-1" |
      | max.decompressed.message.bytes | "2147483639" | "2147483639" |
      | errors.deadletterqueue.group.enable | "TRUE" | "true" |
