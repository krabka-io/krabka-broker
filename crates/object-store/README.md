# crabka-object-store

Typed object-store construction and the shared object-operation surface for Crabka.

Part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation of Apache Kafka.

The crate turns an `ObjectStoreConfig` into an `object_store` handle, and it holds the one `ObjectOps` implementation that the KIP-405 tiered storage in `crabka-remote-storage` and the observability blockstore in `crabka-blockstore` both call. `PutRequest` carries the precondition for a write and asks for a fused SHA-256 digest of the payload; `PutOutcome` returns that digest with the size, the entity tag, and the version id. The WORM archive needs both: `PutMode::Create` makes a second write to a key fail instead of replace it, and the digest is what a segment manifest records.

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see [NOTICE](../../NOTICE).
