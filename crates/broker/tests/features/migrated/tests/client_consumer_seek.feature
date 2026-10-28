Feature: tests client consumer seek

  Scenario: seek before first poll resumes from sought offset
    Then client consumer seek: seek before first poll resumes from sought offset

  Scenario: seek rejects negative offset
    Then client consumer seek: seek rejects negative offset
