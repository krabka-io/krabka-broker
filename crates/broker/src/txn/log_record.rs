//! Byte-exact codec for Kafka's `TransactionLogValue` and `TransactionLogKey`.
//!
//! The codec covers `TransactionLogValue` v0 and v1, and `TransactionLogKey`
//! v0. It matches the on-disk records that the `__transaction_state` topic
//! carries in Apache Kafka 4.0.
//!
//! This module is a codec only. The transaction coordinator owns the runtime
//! wiring.
//!
//! Schema, from cp-kafka 4.0 `TransactionLogValue.json` and
//! `TransactionLogKey.json`:
//!
//! `TransactionLogValue`: validVersions "0-1", flexibleVersions "1+". Wire, in
//! field order: `int16` version, `int64` `ProducerId`, `int16` `ProducerEpoch`,
//! `int32` `TransactionTimeoutMs`, `int8` `TransactionStatus`, a nullable array
//! of `{ string Topic; int32[] PartitionIds }`, `int64`
//! `TransactionLastUpdateTimestampMs`, `int64` `TransactionStartTimestampMs`. v1
//! adds a trailing tagged-field section on every struct; tags 0
//! (`PreviousProducerId`, default -1), 1 (`NextProducerId`, default -1), 2
//! (`ClientTransactionVersion`, default 0), and 3 (`NextProducerEpoch`, default
//! -1) are emitted only when non-default.
//!
//! v0 is non-flexible: arrays use `int32` lengths (-1 = null), strings use
//! `int16` lengths, and there is no tagged-field section anywhere. v1 is
//! flexible: arrays use compact `uvarint(n+1)` lengths (0 = null), strings use
//! compact `uvarint(len+1)` lengths, and every struct ends with a
//! tagged-field section.
//!
//! The two record codecs are one file each: [`key`] carries the
//! `TransactionLogKey` codec and [`value`] the `TransactionLogValue` codec.

mod key;
mod value;

pub(crate) use self::{
    key::{decode_key, encode_key},
    value::{decode_value, encode_value},
};
