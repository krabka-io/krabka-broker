//! The shared subscription loop that applies the latest published `MetadataImage`
//! into an image-derived cache, kept separate from the metadata traits because
//! it is the one piece of this module that neither reads nor writes the
//! controller log itself.

use {
    krabka_metadata::MetadataImage,
    std::{future, sync::Arc, time::Duration},
    tokio::{sync::watch, time},
    tokio_util::sync::CancellationToken,
};

/// Apply `apply` to the current image and each observed latest image until
/// shutdown or the publisher closes. Rapid updates can coalesce.
/// `task` names the loop in the shutdown log.
///
/// Every broker runs one of these per image-derived cache (throttle rates,
/// quota buckets), not only the controller leader. Taking the receiver rather
/// than the whole [`MetadataSource`](super::MetadataSource) keeps the read of
/// latest image on the same channel that woke the loop. Intermediate images
/// can be skipped, so the callback should rebuild from the complete image.
pub async fn watch_image_loop(
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

/// The next owned publication on this receiver, or no image when its publisher closes.
pub async fn next_published_image(
    images: &mut watch::Receiver<Arc<MetadataImage>>,
) -> Option<Arc<MetadataImage>> {
    wait_for_image_change(Some(images))
        .await
        .then(|| images.borrow_and_update().clone())
}

/// Wait for a publication until cancellation or the publisher closing.
pub async fn next_image_until_shutdown(
    images: &mut watch::Receiver<Arc<MetadataImage>>,
    shutdown: &CancellationToken,
) -> Option<Arc<MetadataImage>> {
    tokio::select! {
        () = shutdown.cancelled() => None,
        image = next_published_image(images) => image,
    }
}

/// Wait for a subscribed image update; no subscription leaves that select arm pending.
pub async fn wait_for_image_change(
    images: Option<&mut watch::Receiver<Arc<MetadataImage>>>,
) -> bool {
    match images {
        Some(images) => images.changed().await.is_ok(),
        None => future::pending().await,
    }
}

/// Whether `topic` is in the image on `images`, now or within `timeout`.
///
/// A node that forwards its writes sees a topic another node created only
/// once its own image catches up with the leader. A closed publisher counts
/// as the topic never arriving.
pub async fn topic_reaches_image(
    mut images: watch::Receiver<Arc<MetadataImage>>,
    topic: &str,
    timeout: Duration,
) -> bool {
    time::timeout(
        timeout,
        images.wait_for(|image| image.topic(topic).is_some()),
    )
    .await
    .is_ok_and(|seen| seen.is_ok())
}

#[cfg(test)]
mod tests {
    use {super::*, crate::test_support::topic_image, tokio::sync::mpsc};

    const WAIT: Duration = Duration::from_secs(5);

    /// The topic counts as arrived when it is in the image already, or when
    /// an image that holds it is published before the timeout; not when the
    /// timeout passes first or the publisher closes.
    #[tokio::test(start_paused = true)]
    async fn topic_reaches_image_waits_for_the_topic_until_the_timeout() {
        let empty = || Arc::new(MetadataImage::new(uuid::Uuid::nil()));
        let with_topic = || Arc::new(topic_image().topic("orders").call());

        let (_tx, rx) = watch::channel(with_topic());
        assert2::assert!(topic_reaches_image(rx, "orders", Duration::ZERO).await);

        let (tx, rx) = watch::channel(empty());
        let waiting = tokio::spawn(async move { topic_reaches_image(rx, "orders", WAIT).await });
        time::sleep(WAIT / 2).await;
        tx.send(with_topic()).expect("the wait is still subscribed");
        assert2::assert!(waiting.await.expect("the wait does not panic"));

        let (_tx, rx) = watch::channel(with_topic());
        assert2::assert!(!topic_reaches_image(rx, "other", WAIT).await);

        let (tx, rx) = watch::channel(empty());
        drop(tx);
        assert2::assert!(!topic_reaches_image(rx, "orders", WAIT).await);
    }

    /// The loop both callers share: it must apply the image already in the
    /// channel before waiting, apply each update observed in this test, and stop
    /// on cancellation rather than on the next publish.
    #[tokio::test]
    async fn watch_image_loop_applies_current_then_each_published_image() {
        use {
            super::watch_image_loop, krabka_metadata::MetadataImage,
            tokio_util::sync::CancellationToken,
        };

        let image = |n: u128| Arc::new(MetadataImage::new(uuid::Uuid::from_u128(n)));
        let (tx, rx) = watch::channel(image(1));
        // Awaiting each apply, rather than polling a shared Vec, is what makes
        // this deterministic on the current-thread test runtime: the loop task
        // is not polled until this task parks, so a publish sent before that
        // first park would be the image the loop reads as its "current" one.
        let (applied_tx, mut applied) = mpsc::unbounded_channel();
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
