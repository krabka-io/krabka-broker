Feature: Tiered local retention rolls the active segment only after a deletable sealed prefix
  The active segment starts at offset 20. A null active tail means the high
  watermark has not passed its log end, so the walk cannot roll it.

  Scenario Outline: Decide retention when <case>
    Given the tiered local log footprint, copied frontier and retention policy are JSON <footprint>
    When released immediate-topic local retention walks the log at millisecond 10000 with active base offset 20
    Then the complete local retention decision is JSON <decision>

    Examples:
      | case | footprint | decision |
      | A lone active segment past the window rolls | {"sealed":[],"active":{"timestamp":100,"size":50},"covered_through":null,"retention_ms":1000,"retention_bytes":null} | {"delete_through":null,"roll_active":true} |
      | A lone active segment inside the window stays | {"sealed":[],"active":{"timestamp":9500,"size":50},"covered_through":null,"retention_ms":1000,"retention_bytes":null} | {"delete_through":null,"roll_active":false} |
      | An empty active segment never rolls | {"sealed":[],"active":{"timestamp":100,"size":0},"covered_through":null,"retention_ms":1000,"retention_bytes":null} | {"delete_through":null,"roll_active":false} |
      | An active segment above the high watermark never rolls | {"sealed":[],"active":null,"covered_through":null,"retention_ms":1000,"retention_bytes":null} | {"delete_through":null,"roll_active":false} |
      | Copied breached segments go and the active tail rolls | {"sealed":[{"base":0,"last":9,"timestamp":100,"size":100},{"base":10,"last":19,"timestamp":200,"size":100}],"active":{"timestamp":300,"size":50},"covered_through":19,"retention_ms":1000,"retention_bytes":null} | {"delete_through":20,"roll_active":true} |
      | A sealed segment missing from the tier stops the walk | {"sealed":[{"base":0,"last":9,"timestamp":100,"size":100},{"base":10,"last":19,"timestamp":200,"size":100}],"active":{"timestamp":300,"size":50},"covered_through":9,"retention_ms":1000,"retention_bytes":null} | {"delete_through":10,"roll_active":false} |
      | A sealed segment inside the time window stops the walk | {"sealed":[{"base":0,"last":9,"timestamp":100,"size":100},{"base":10,"last":19,"timestamp":9500,"size":100}],"active":{"timestamp":300,"size":50},"covered_through":19,"retention_ms":1000,"retention_bytes":null} | {"delete_through":10,"roll_active":false} |
      | Size debt covering the active segment rolls it | {"sealed":[{"base":0,"last":9,"timestamp":100,"size":100},{"base":10,"last":19,"timestamp":200,"size":100}],"active":{"timestamp":9500,"size":50},"covered_through":19,"retention_ms":null,"retention_bytes":0} | {"delete_through":20,"roll_active":true} |
      | Size debt short of the active segment keeps it | {"sealed":[{"base":0,"last":9,"timestamp":100,"size":100},{"base":10,"last":19,"timestamp":200,"size":100}],"active":{"timestamp":9500,"size":50},"covered_through":19,"retention_ms":null,"retention_bytes":50} | {"delete_through":20,"roll_active":false} |
