Feature: Match client metrics subscriptions and bound push intervals

  Scenario Outline: Compute metrics and push frequency for one client
    Given client metrics subscriptions and client attributes <input>
    When the broker computes the client's metrics subscription
    Then the client receives metrics <metrics> at interval <interval> milliseconds

    Examples:
      | input | metrics | interval |
      | {"subscriptions":[["s1",[["metrics","a."],["interval.ms","600000"]]]],"client_id":"svc-1","software_name":"apache-kafka-java"} | ["a."] | 300000 |
      | {"subscriptions":[["s1",[["metrics","a."],["interval.ms","60000"]]],["s2",[["metrics",""],["interval.ms","1000"]]]],"client_id":"svc-1","software_name":"apache-kafka-java"} | ["a."] | 1000 |
      | {"subscriptions":[],"client_id":"svc-1","software_name":"apache-kafka-java"} | [] | 300000 |
      | {"subscriptions":[["s1",[["metrics","*"],["match","client_id=app\|app-1"]]]],"client_id":"app-1","software_name":"apache-kafka-java"} | ["*"] | 300000 |
      | {"subscriptions":[["s1",[["metrics","*"],["match","client_id=app"]]]],"client_id":"app-1","software_name":"apache-kafka-java"} | [] | 300000 |
      | {"subscriptions":[["s1",[["metrics","*"],["match","client_software_name=unknown"]]]],"client_id":"svc-1","software_name":"unknown"} | ["*"] | 300000 |
      | {"subscriptions":[["s1",[["metrics","*"],["match","client_id=a,client_id=b"]]]],"client_id":"b","software_name":"apache-kafka-java"} | ["*"] | 300000 |
