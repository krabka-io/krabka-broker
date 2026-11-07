Feature: The tool tells an operator what happened
  Each outcome ends on one exit code that a runbook branches on: 0 when the
  broker accepted the request, 1 when it refused it, 2 when the broker could
  not be reached, and 3 when a cut does not match the log. The reason goes
  to stderr. KFC-4 is the specification:
  docs/KFCs/KFC-4-cross-topic-snapshots.md.

  Rule: A partial cut is a published outcome, not a failure

    Scenario: A cut that missed one partition succeeds and names the gap
      When the broker answers a trigger with a partial cut that has no marker in payments partition 1
      Then the tool succeeds and reports no marker in payments partition 1

  Rule: An injection already in flight reads as a hint to retry

    Scenario: A trigger refused because an injection is running tells the operator to retry
      When the broker refuses a trigger because an injection is already in flight
      Then the tool exits 1 and tells the operator that an injection is already in flight, retry

    Scenario: A refusal with a Kafka error code names the code and the broker's reason
      When the broker refuses a trigger with error 91 and the reason no such barrier group
      Then the tool exits 1 and reports error 91: no such barrier group

  Rule: A broker that cannot be reached is not a refusal

    Scenario: A request to an address where no broker listens ends on exit code 2
      When an operator triggers a cut against an address where no broker listens
      Then the tool exits 2 because the outcome of the request is unknown
