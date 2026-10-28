Feature: tests jvm acceptance reassign preferred election

  Scenario: jvm kafka leader election preferred
    Then jvm acceptance reassign preferred election: jvm kafka leader election preferred

  Scenario: election selectors report election not needed as apache kafka does
    Then jvm acceptance reassign preferred election: election selectors report election not needed as apache kafka does

  Scenario: a preferred replica outside the isr reports code 80
    Then jvm acceptance reassign preferred election: a preferred replica outside the isr reports code 80
