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

  Rule: A configured S3 setting wins over the broker's AWS environment

    Scenario Outline: The AWS environment does not override a configured S3 setting
      Given an operator has configured the s3 backend with <configured>
      And the broker's environment sets <variable> to <environment>
      When the broker resolves the S3 store's settings
      Then the store's <property> is <expected>

      Examples:
        | case                              | configured             | variable            | environment             | property      | expected                 |
        | the configured region wins        | its essential settings | AWS_REGION          | eu-west-1               | region        | us-east-1                |
        | the configured endpoint wins      | an endpoint            | AWS_ENDPOINT_URL_S3 | https://s3.example.test | endpoint      | http://minio:9000        |
        | the configured access key id wins | a static access key    | AWS_ACCESS_KEY_ID   | environment-key-id      | access key id | operator-credential-7f3a |

    Scenario: A configured static access key does not take the environment's session token
      Given an operator has configured the s3 backend with a static access key
      And the broker's environment sets AWS_SESSION_TOKEN to environment-session-token
      When the broker resolves the S3 store's settings
      Then the store has no session token

    Scenario: A configured static access key wins over a web identity in the environment
      Given an operator has configured the s3 backend with a static access key
      And the broker's environment names a web identity credential whose token file is missing
      When the broker asks the S3 store for a credential
      Then the store signs with the configured access key

  Rule: The broker's AWS environment fills an S3 setting the operator left unset

    Scenario Outline: An unset S3 setting is taken from the AWS environment
      Given an operator has configured the s3 backend with <configured>
      And the broker's environment sets <variable> to <environment>
      When the broker resolves the S3 store's settings
      Then the store's <property> is <environment>

      Examples:
        | case                               | configured             | variable                               | environment                                             | property                   |
        | the region from AWS_REGION         | no region              | AWS_REGION                             | eu-west-1                                               | region                     |
        | the region from AWS_DEFAULT_REGION | no region              | AWS_DEFAULT_REGION                     | eu-central-1                                            | region                     |
        | the endpoint from the S3 endpoint  | its essential settings | AWS_ENDPOINT_URL_S3                    | https://s3.example.test                                 | endpoint                   |
        | the IRSA token file                | its essential settings | AWS_WEB_IDENTITY_TOKEN_FILE            | /var/run/secrets/eks.amazonaws.com/serviceaccount/token | web identity token file    |
        | the IRSA role                      | its essential settings | AWS_ROLE_ARN                           | arn:aws:iam::123456789012:role/krabka-tier              | role ARN                   |
        | the ECS task role                  | its essential settings | AWS_CONTAINER_CREDENTIALS_RELATIVE_URI | /v2/credentials/krabka                                  | container credentials path |

    Scenario: AWS_REGION wins over AWS_DEFAULT_REGION
      Given an operator has configured the s3 backend with no region
      And the broker's environment sets AWS_DEFAULT_REGION to eu-central-1
      And the broker's environment sets AWS_REGION to eu-west-1
      When the broker resolves the S3 store's settings
      Then the store's region is eu-west-1

    Scenario: An S3 store whose region nothing names signs for us-east-1
      Given an operator has configured the s3 backend with no region
      When the broker resolves the S3 store's settings
      Then the store's region is us-east-1

  Rule: An S3 store without a static key takes its credential from the AWS environment

    Scenario Outline: The store reads the token file the environment names
      Given an operator has configured the s3 backend with its essential settings
      And the broker's environment names a <provider> credential whose token file is missing
      When the broker asks the S3 store for a credential
      Then the credential request fails on the missing token file

      Examples:
        | case             | provider         |
        | IRSA on EKS      | web identity     |
        | EKS Pod Identity | EKS Pod Identity |
