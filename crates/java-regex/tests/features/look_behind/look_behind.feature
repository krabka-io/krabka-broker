Feature: A Kerberos substitution reads a look-behind as Java does

  The s/pattern/replacement/ part of a sasl.kerberos.principal.to.local.rules
  entry is Java's String.replaceFirst. Replacement syntax is in
  ../replacement/replacement.feature.

  Rule: A look-behind checks the text before a match without replacing it

    Scenario: The realm after an at sign is replaced and the at sign is kept
      Given a Kerberos substitution s/(?<=@)EXAMPLE\.COM/CORP/
      When the substitution rewrites the name alice@EXAMPLE.COM
      Then the rewritten name is alice@CORP

    Scenario: A realm without the required text before it is left alone
      Given a Kerberos substitution s/(?<=admin@)EXAMPLE\.COM/CORP/
      When the substitution rewrites the name alice@EXAMPLE.COM
      Then the rewritten name is alice@EXAMPLE.COM

  Rule: A look-behind may have alternatives of different lengths

    Scenario: The longer alternative of a look-behind is tried
      Given a Kerberos substitution s/(?<=@|@ops\.)CORP/EU/
      When the substitution rewrites the name alice@ops.CORP
      Then the rewritten name is alice@ops.EU

  Rule: Java refuses a look-behind with no obvious maximum length

    Scenario: A look-behind with a backreference is refused
      When an operator writes the Kerberos substitution s/(a)(?<=\1)b//
      Then the substitution is refused as an invalid pattern

    Scenario: A look-behind with an unbounded repeat is accepted
      When an operator writes the Kerberos substitution s/(?<=a+)b//
      Then the substitution is accepted
