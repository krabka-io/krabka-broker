Feature: An archive store is built from the operator's backend configuration

  The broker turns the [remote_storage] table of its configuration file, in
  crates/broker/src/file_config, into one backend configuration and builds
  the archive store that tiered storage, the WORM archive and the
  observability blockstore write to. Building a store checks the
  configuration; it does not reach the bucket.

  Rule: A complete backend configuration builds an archive store

    Scenario Outline: Each backend builds from its essential settings
      Given an operator has configured the <backend> backend with its essential settings
      When the broker attempts to build the archive store
      Then the archive store is built

      Examples:
        | case                                        | backend          |
        | the test store needs no settings            | in-memory        |
        | the development store needs a root          | local-filesystem |
        | an S3 bucket needs a bucket and a region    | s3               |
        | a GCS bucket uses keyless workload identity | gcs              |

  Rule: A local-filesystem store needs a root directory that exists

    Scenario: A local-filesystem store rooted at a missing directory is refused
      Given an operator has configured the local-filesystem backend at a directory that does not exist
      When the broker attempts to build the archive store
      Then the build is refused as an invalid local configuration

  Rule: A GCS store takes at most one service-account credential

    Scenario Outline: A GCS store given one service-account credential builds
      Given an operator has configured the gcs backend with <credential>
      When the broker attempts to build the archive store
      Then the archive store is built

      Examples:
        | case                  | credential                    |
        | a key file on disk    | a service-account key file    |
        | a key inline          | an inline service-account key |

    Scenario: A GCS store given both a service-account key file and an inline key is refused
      Given an operator has configured the gcs backend with a service-account key file
      And the operator has also configured an inline service-account key
      When the broker attempts to build the archive store
      Then the build is refused as an invalid GCS configuration

  Rule: An S3 access key id is only used together with its secret access key

    Scenario: An S3 store with an access key id and its secret builds
      Given an operator has configured the s3 backend with an access key id
      And the operator has also configured its secret access key
      When the broker attempts to build the archive store
      Then the archive store is built

    Scenario Outline: An S3 store given half of a static credential is refused
      Given an operator has configured the s3 backend with <half>
      When the broker attempts to build the archive store
      Then the build is refused as an invalid S3 configuration

      Examples:
        | case                      | half                |
        | the secret is missing     | an access key id    |
        | the access key is missing | a secret access key |

  Rule: A bucket-backed store needs a bucket name

    @inprogress
    Scenario Outline: A bucket-backed store with an empty bucket name is refused
      Given an operator has configured the <backend> backend with an empty bucket name
      When the broker attempts to build the archive store
      Then the build is refused as an invalid <backend> configuration

      Examples:
        | case         | backend |
        | an S3 bucket | s3      |
        | a GCS bucket | gcs     |

  Rule: A printed backend configuration never shows a credential

    Scenario Outline: A credential is redacted when the configuration is printed
      Given an operator has configured the <backend> backend with <credential>
      When the broker prints the backend configuration
      Then the printed configuration hides the credential

      Examples:
        | case                                    | backend | credential                      |
        | the S3 key id                           | s3      | an access key id                |
        | the S3 secret                           | s3      | a secret access key             |
        | the GCS service-account file            | gcs     | a service-account key file      |
        | the GCS inline key                      | gcs     | an inline service-account key   |
        | the GCS application default credentials | gcs     | an application credentials file |

  Rule: An operator who sets nothing gets the documented S3 connection policy

    Scenario Outline: An unset S3 setting takes its documented default
      Given an operator has configured the s3 backend with its essential settings
      When the broker resolves the backend configuration
      Then the default <setting> is <value>

      Examples:
        | case                                | setting                      | value     |
        | uploads switch to parts at 100 MiB  | multipart threshold in bytes | 104857600 |
        | each part is 16 MiB                 | multipart part size in bytes | 16777216  |
        | a request is retried ten times      | retry count                  | 10        |
        | retries give up after three minutes | retry timeout in seconds     | 180       |
        | one request may take thirty seconds | request timeout in seconds   | 30        |
        | a connection may take five seconds  | connect timeout in seconds   | 5         |
        | create-only writes are atomic       | conditional put              | on        |
        | the bucket verifies each object     | SHA-256 checksum             | on        |
