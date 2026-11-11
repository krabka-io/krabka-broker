Feature: KRaft quorum recovery under network hardship
  Rule: Followers refuse metadata writes with a leader hint
    Scenario Outline: A follower identifies the elected leader
      Given three KRaft voters have elected a leader under Turmoil seed <seed>
      When a follower receives topic refused with identity 99
      Then the follower refuses the submission with the elected leader hint
      And no voter exposes topic refused

      Examples:
        | case                     | seed |
        | baseline delivery order  | 1    |
        | alternate delivery order | 7    |
        | alternate host order     | 42   |

  Rule: A leader replicates committed metadata to every voter
    Scenario Outline: Every voter applies the leader's topic identity
      Given three KRaft voters have elected a leader under Turmoil seed <seed>
      When the elected leader receives topic orders with identity 1
      Then the leader acknowledges the submission
      And every voter contains topic orders with identity 1

      Examples:
        | case                     | seed |
        | baseline delivery order  | 1    |
        | alternate delivery order | 7    |
        | alternate host order     | 42   |

  Rule: A minority cannot commit metadata
    Scenario Outline: An isolated leader keeps its new topic invisible
      Given three KRaft voters have elected a leader under Turmoil seed <seed>
      And topic orders with identity 1 is committed on every voter
      And the original leader host is disconnected from both followers
      When the isolated leader receives topic isolated with identity 99
      Then the isolated write remains uncommitted

      Examples:
        | case                     | seed |
        | baseline delivery order  | 1    |
        | alternate delivery order | 7    |
        | alternate host order     | 42   |

  Rule: A majority can commit while a voter is isolated
    Scenario Outline: The replacement leader commits beyond the minority write
      Given three KRaft voters have elected a leader under Turmoil seed <seed>
      And the majority has replaced the leader that received isolated topic isolated with identity 99
      When the replacement leader receives topic payments with identity 2
      Then the leader acknowledges the submission
      And the isolated voter retains only this committed metadata
        | topic  | identity |
        | orders | 1        |

      Examples:
        | case                     | seed |
        | baseline delivery order  | 1    |
        | alternate delivery order | 7    |
        | alternate host order     | 42   |

  Rule: A returning minority accepts the majority's committed metadata
    Scenario Outline: Repair refuses the old submission without losing committed topics
      Given three KRaft voters have elected a leader under Turmoil seed <seed>
      And the majority has committed topic payments with identity 2 beyond an isolated minority write
      When the minority resumes replication of topic payments
      Then the original isolated submission is refused after reconciliation
      And all voters agree on this recovered metadata
        | topic    | identity |
        | orders   | 1        |
        | payments | 2        |

      Examples:
        | case                     | seed |
        | baseline delivery order  | 1    |
        | alternate delivery order | 7    |
        | alternate host order     | 42   |

  Rule: Restarted voters recover acknowledged metadata from their own logs
    Scenario Outline: The crashed leader recovers its topic without contacting peers
      Given three KRaft voters have elected a leader under Turmoil seed <seed>
      And a crashed leader is isolated from its replacement quorum
      When the crashed host restarts from its existing log directory
      Then the restarted voter recovers exactly this metadata before contacting peers
        | topic  | identity |
        | orders | 1        |

      Examples:
        | case                     | seed |
        | baseline delivery order  | 1    |
        | alternate delivery order | 7    |
        | alternate host order     | 42   |

  Rule: Restarted voters catch up with the replacement quorum
    Scenario Outline: The recovered leader applies the replacement's topic
      Given three KRaft voters have elected a leader under Turmoil seed <seed>
      And the original leader has restarted without peer access
      When the minority resumes replication of topic payments
      Then all voters agree on this recovered metadata
        | topic    | identity |
        | orders   | 1        |
        | payments | 2        |

      Examples:
        | case                     | seed |
        | baseline delivery order  | 1    |
        | alternate delivery order | 7    |
        | alternate host order     | 42   |
