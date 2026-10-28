Feature: Metadata classifies replica availability from registration fencing and directory identity
  Scenario Outline: Offline replicas retain replica order and recognize unassigned directory sentinels
    Given replicas <replicas> use directories <directories> with registrations <registrations> and unavailable brokers <unavailable>
    When the metadata response projects offline replicas for the PLAINTEXT listener
    Then the offline replica IDs in replica order are <expected>
    Examples:
      | replicas | directories | registrations          | unavailable | expected |
      | 1,2      | good,good   | 1=good,bad;2=good       | none        | none     |
      | 1,2      | bad,good    | 1=good;2=good           | none        | 1        |
      | 1,2      | good,good   | 1=good;2=good           | 2           | 2        |
      | 1,2      | good,good   | 1=good                  | none        | 2        |
      | 1,2      | nil,nil     | 1=good;2=good           | none        | none     |
      | 1,2      | bad,good    | 1=none;2=good           | none        | 1        |
      | 1,2      | nil,good    | 1=none;2=good           | none        | none     |
      | 1,2      | none        | 1=good;2=good           | none        | none     |
      | 2,1      | good,bad    | 1=good;2=good           | 2           | 2,1      |
