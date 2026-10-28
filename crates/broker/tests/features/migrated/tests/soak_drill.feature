Feature: The secured recovery drill preserves a surviving source's acknowledged records

  Scenario: the drill reads back every record a surviving source acknowledged
    Given the source broker serves SASL_SSL with SCRAM-SHA-512 user soak and password qualification-soak using the development TLS certificate
    When the objectives command prints bench/soak/dr-secured.yaml with post-capture load 2s and throughput floor 1/s
    Then the printed objectives exactly match the parsed contract's declared objectives
    Given every path declared by the objectives has its required topic and partition count
    When the load command authenticates over TLS and runs for fourteen seconds before source loss is signalled
    Then the load command prints nothing and every group has acknowledged a commit over two seconds before source loss
    When readback authenticates to the surviving source as the restored cluster, using the acknowledged-commit capture boundary and serving time one second after loss
    Then readback prints nothing, reconciliation has no findings, recovery has RPO 0 and RTO 1000ms with no loss or problems, and each path has records on both sides of capture
    When the probe command writes and reads partition 0 of every declared path over the same TLS login
    Then every probe reports its topic partition 0 wrote and read back an offset
