Feature: tests soak contract host

  Scenario Outline: tests: the evidence goes where the runner asks
    Then soak contract host tests: the evidence goes where the runner asks for <example>

    Examples:
      | example                           |
      | explicit wins                     |
      | bazel outputs                     |
      | neither                           |
      | empty is unset                    |
      | empty does not hide bazel outputs |
      | empty bazel outputs is unset      |

  Scenario Outline: tests: memory is read in kibibytes
    Then soak contract host tests: memory is read in kibibytes for <example>

    Examples:
      | example    |
      | kibibytes  |
      | absent     |
      | other unit |

  Scenario Outline: tests: free space is the number under the header
    Then soak contract host tests: free space is the number under the header for <example>

    Examples:
      | example     |
      | plain       |
      | header only |
      | garbage     |
