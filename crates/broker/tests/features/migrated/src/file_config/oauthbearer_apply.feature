Feature: src file config oauthbearer apply

  Scenario Outline: tests: file config validator applies the kafka clock skew default
    Then file config oauthbearer apply tests: file config validator applies the kafka clock skew default for <example>

    Examples:
      | example                                               |
      | unsecured default refuses one second past exp         |
      | unsecured explicit skew admits one second past exp    |
      | unsecured explicit skew refuses past the skew         |
      | signed default admits one second past exp             |
      | signed default admits just inside thirty seconds      |
      | signed default refuses past thirty seconds            |
      | signed explicit zero skew refuses one second past exp |

  Scenario Outline: tests: file config unsecured validator applies the kafka scope options
    Then file config oauthbearer apply tests: file config unsecured validator applies the kafka scope options for <example>

    Examples:
      | example                                          |
      | no required scope admits a token without scope   |
      | every required item admits                       |
      | a missing item is insufficient scope             |
      | a blank required scope requires none             |
      | the custom claim name is read                    |
      | the default claim is ignored under a custom name |
      | a numeric custom claim is an invalid token       |
