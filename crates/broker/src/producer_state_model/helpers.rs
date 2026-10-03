use super::*;

/// Kafka's classification of `batch` at `epoch` against `window`:
/// `UnifiedLog.analyzeAndValidateProducerState` looks the batch up among the
/// retained batches first (`ProducerStateEntry.findDuplicateBatch`, same epoch
/// only), then `ProducerAppendInfo.checkProducerEpoch` and `checkSequence`
/// decide the rest.
pub(super) fn kafka_decision(window: Option<&Window>, epoch: i16, batch: Range) -> Answer {
    let Some(window) = window else {
        return Answer::Append;
    };
    if epoch == window.epoch && window.retained.contains(&batch) {
        return Answer::Duplicate(batch);
    }
    if epoch < window.epoch {
        Answer::Fenced
    } else if epoch > window.epoch {
        if batch.base == 0 {
            Answer::Append
        } else {
            Answer::OutOfOrder
        }
    } else if batch.base == window.last_sequence + 1 {
        Answer::Append
    } else {
        Answer::OutOfOrder
    }
}
