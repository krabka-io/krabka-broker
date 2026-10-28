Feature: A remote share coordinator samples committed data rather than uncommitted log end

  Scenario: rf three remote leader uses committed high watermark
    Given an initialized RF3 share group has backlog 0 and its coordinator differs from the data leader
    When the third replica stops and the data leader appends five records that cannot reach the committed high watermark
    Then a fresh backlog sample is 0 while all three replicas remain in ISR and only the coordinator exports the series
