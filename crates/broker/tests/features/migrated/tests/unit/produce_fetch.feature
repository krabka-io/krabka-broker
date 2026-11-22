Feature: Kafka record offsets and fetch round trips

  Rule: An append starts at the next available offset
    Scenario Outline: Appends follow the records already stored
      Given a single broker for record requests
      And an empty single-partition topic named prod
      And the topic already contains <stored> records
      When the client produces <appended> records to the topic
      Then the latest produce response contains one successful partition at offset <offset>

      Examples:
        | case               | stored | appended | offset |
        | first append       | 0      | 3        | 0      |
        | subsequent append  | 3      | 2        | 3      |

  Rule: A produce request must identify its topic
    Scenario: Producing without a topic id is refused
      Given a single broker for record requests
      When the client produces without a topic id
      Then produce without a topic id returns UNKNOWN_TOPIC_ID

  Rule: Fetch exposes the stored records
    Scenario: Fetch reads the records from offset zero
      Given a single broker for record requests
      And an empty single-partition topic named round
      And the topic already contains 3 records
      When the client fetches the topic from offset zero
      Then fetch returns the topic partition successfully at offsets 0,1,2

  Rule: Offset bounds describe the available log
    Scenario Outline: Earliest and latest offsets bound the stored records
      Given a single broker for record requests
      And an empty single-partition topic named bounds
      And the topic already contains <stored> records
      When the client requests the <bound> offset
      Then the requested offset is <offset> without a Kafka error

      Examples:
        | case                  | stored | bound    | offset |
        | empty log start       | 0      | earliest | 0      |
        | empty log end         | 0      | latest   | 0      |
        | populated log start   | 3      | earliest | 0      |
        | populated log end     | 3      | latest   | 3      |
