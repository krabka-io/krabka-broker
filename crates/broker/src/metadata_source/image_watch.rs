//! The shared subscription loop that pushes every published `MetadataImage`
//! into an image-derived cache, kept separate from the metadata traits because
//! it is the one piece of this module that neither reads nor writes the
//! controller log itself.

use std::sync::Arc;

use krabka_metadata::MetadataImage;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

/// Apply `apply` to the image `images` currently holds, then to every image
/// published after it, until `shutdown` fires or the publisher goes away.
/// `task` names the loop in the shutdown log.
///
/// Every broker runs one of these per image-derived cache (throttle rates,
/// quota buckets), not only the controller leader. Taking the receiver rather
/// than the whole [`MetadataSource`](super::MetadataSource) keeps the read of
/// the new image on the same channel that woke the loop, so no image can slip
/// past between the wake-up and the read.
pub(crate) async fn watch_image_loop(
    mut images: watch::Receiver<Arc<MetadataImage>>,
    task: &str,
    shutdown: CancellationToken,
    mut apply: impl FnMut(&MetadataImage) + Send,
) {
    apply(&images.borrow_and_update());
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => {
                tracing::info!(task, "image watch task shutting down");
                return;
            }
            changed = images.changed() => {
                if changed.is_err() {
                    tracing::info!(task, "image watch task: image channel closed");
                    return;
                }
            }
        }
        apply(&images.borrow_and_update());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The loop both callers share: it must apply the image already in the
    /// channel before waiting, apply every image published after it, and stop
    /// on cancellation rather than on the next publish.
    #[tokio::test]
    async fn watch_image_loop_applies_current_then_each_published_image() {
        use krabka_metadata::MetadataImage;
        use tokio_util::sync::CancellationToken;

        use super::watch_image_loop;

        let image = |n: u128| Arc::new(MetadataImage::new(uuid::Uuid::from_u128(n)));
        let (tx, rx) = watch::channel(image(1));
        // Awaiting each apply, rather than polling a shared Vec, is what makes
        // this deterministic on the current-thread test runtime: the loop task
        // is not polled until this task parks, so a publish sent before that
        // first park would be the image the loop reads as its "current" one.
        let (applied_tx, mut applied) = tokio::sync::mpsc::unbounded_channel();
        let shutdown = CancellationToken::new();

        let task = tokio::spawn(watch_image_loop(rx, "test", shutdown.clone(), move |img| {
            applied_tx.send(img.cluster_id()).expect("receiver is live");
        }));

        assert2::assert!(applied.recv().await == Some(uuid::Uuid::from_u128(1)));
        for n in 2..=4 {
            tx.send(image(n)).expect("loop is still receiving");
            assert2::assert!(applied.recv().await == Some(uuid::Uuid::from_u128(n)));
        }

        shutdown.cancel();
        task.await.expect("loop exits on cancellation");
        assert2::assert!(applied.recv().await == None, "no apply after shutdown");
    }
}
