Feature: src format tests

  Scenario Outline: the first error follows kafkas order
    Then format tests: the first error follows kafkas order for <example>

    Examples:
      | example               |
      | unknown release first |
      | unknown feature first |
      | mode conflict first   |
      | undefined kraft level |

  Scenario Outline: the metadata log directory leads the directory set
    Then format tests: the metadata log directory leads the directory set for <example>

    Examples:
      | example                  |
      | default first log dir    |
      | separate metadata dir    |
      | metadata is log dir      |
      | metadata is only log dir |
      | duplicate log dir        |
      | voter                    |
      | non voting controller    |

  Scenario Outline: only the metadata log directory gets the bootstrap files
    Then format tests: only the metadata log directory gets the bootstrap files for <example>

    Examples:
      | example                     |
      | static separate metadata    |
      | dynamic separate metadata   |
      | dynamic first log dir       |
      | dynamic metadata is log dir |

  Scenario Outline: an unreadable metadata log directory is named
    Then format tests: an unreadable metadata log directory is named for <example>

    Examples:
      | example                          |
      | unreadable first log dir         |
      | unreadable separate metadata dir |
      | unreadable data dir              |
