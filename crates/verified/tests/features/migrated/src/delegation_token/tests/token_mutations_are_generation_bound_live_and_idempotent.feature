Feature: src delegation token tests token mutations are generation bound live and idempotent

  Scenario Outline: expire matches kafka delete expiry and saturation
    Then delegation token tests token mutations are generation bound live and idempotent: expire matches kafka delete expiry and saturation for <example>

    Examples:
      | example                         |
      | zero period at zero             |
      | zero period                     |
      | positive period                 |
      | period capped at max            |
      | near overflow is capped         |
      | saturating overflow             |
      | zero period at expiry           |
      | zero period at max              |
      | negative period deletes live    |
      | negative period deletes expired |
      | minimum period deletes          |
      | current expiry is past          |
      | max timestamp is past           |
