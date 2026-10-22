//! The controller's applied [`MetadataImage`], the snapshots of it that
//! readers share, and the overlays a submission validates against.
//!
//! A deep copy of the image costs time and memory in proportion to the whole
//! cluster: every topic, partition, broker and config it holds. A small update,
//! such as a diskless offset reservation, should cost in proportion to the
//! update. Two places used to copy the whole image for one:
//!
//! - Publication. Readers hold an immutable snapshot, so the engine cannot
//!   mutate the snapshot it published. [`AppliedImage::publish`] moves the
//!   applied image into the new snapshot instead of copying it, and rebuilds
//!   the engine's own image from the snapshot that the new one replaces. It
//!   replays onto that snapshot the records applied since it was published. A
//!   snapshot that a reader still holds is never touched; the engine then
//!   copies the new snapshot, as it always did.
//! - Submission. [`AppliedImage::overlay`] gives a copy-on-write view. A batch
//!   validates and encodes each record against the image the records before it
//!   produce, so a record that a later one depends on is applied to the view,
//!   and only that copies the image. The last record of a batch is applied to
//!   nothing, so a one-record batch copies nothing.

#[cfg(test)]
use std::cell::Cell;
use {
    krabka_metadata::{MetadataImage, MetadataRecord},
    std::{borrow::Cow, ops::Deref, sync::Arc},
    tokio::sync::watch,
};

/// The most records [`AppliedImage`] keeps to replay at the next publish.
///
/// A pass that applies more than this, such as a follower catching up, copies
/// the image at its publish instead. The bound keeps the replay log from
/// growing with the size of the pass.
const MAX_REPLAY_RECORDS: usize = 16_384;

/// The applied image and the channel that publishes snapshots of it.
///
/// It dereferences to the image for reads. Every write goes through
/// [`Self::apply`] or [`Self::replace`], so the replay log cannot miss one.
pub(super) struct AppliedImage {
    image: MetadataImage,
    tx: watch::Sender<Arc<MetadataImage>>,
    /// The records applied since the published snapshot was taken, in apply
    /// order. `None` when they no longer describe the difference: the image
    /// was replaced, or more records applied than the log keeps.
    since_publish: Option<Vec<MetadataRecord>>,
    /// How many deep copies of the image this value has made.
    #[cfg(test)]
    copies: Cell<u64>,
}

impl AppliedImage {
    /// Takes `image` as the applied image and publishes a snapshot of it.
    pub(super) fn new(image: MetadataImage) -> (Self, watch::Receiver<Arc<MetadataImage>>) {
        let (tx, rx) = watch::channel(Arc::new(image.clone()));
        let applied = Self {
            image,
            tx,
            since_publish: Some(Vec::new()),
            #[cfg(test)]
            copies: Cell::new(0),
        };
        (applied, rx)
    }

    /// Applies `record` to the image.
    pub(super) fn apply(&mut self, record: &MetadataRecord) {
        self.image.apply(record);
        match &mut self.since_publish {
            Some(records) if records.len() < MAX_REPLAY_RECORDS => records.push(record.clone()),
            Some(_) => self.since_publish = None,
            None => {}
        }
    }

    /// Replaces the image, for example with a loaded snapshot.
    pub(super) fn replace(&mut self, image: MetadataImage) {
        self.image = image;
        self.since_publish = None;
    }

    /// Publishes the image as an immutable snapshot.
    ///
    /// The image moves into the snapshot. The engine's next image is the
    /// snapshot this one replaces, with the records applied since replayed
    /// onto it. `MetadataImage::apply` is deterministic, so the replay yields
    /// the image that moved. When a reader still holds the replaced snapshot,
    /// or the records do not describe the difference, the next image is a
    /// copy of the new snapshot.
    pub(super) fn publish(&mut self) {
        let snapshot = Arc::new(std::mem::take(&mut self.image));
        let replaced = self.tx.send_replace(Arc::clone(&snapshot));
        self.image = match (Arc::try_unwrap(replaced), self.since_publish.take()) {
            (Ok(mut image), Some(records)) => {
                for record in &records {
                    image.apply(record);
                }
                image
            }
            _ => self.copy_of(&snapshot),
        };
        self.since_publish = Some(Vec::new());
    }

    /// Publishes a new snapshot that holds what the current one holds.
    ///
    /// Readers that compare snapshots by identity see a change. The snapshot's
    /// contents, and so the replay log, stay as they were.
    pub(super) fn republish(&self) {
        let published = self.copy_of(&self.tx.borrow());
        self.tx.send_replace(Arc::new(published));
    }

    /// The published snapshot.
    pub(super) fn published(&self) -> watch::Ref<'_, Arc<MetadataImage>> { self.tx.borrow() }

    /// A receiver of published snapshots.
    #[cfg(test)]
    pub(super) fn subscribe(&self) -> watch::Receiver<Arc<MetadataImage>> { self.tx.subscribe() }

    /// A copy-on-write view of the image.
    pub(super) fn overlay(&self) -> Overlay<'_> {
        Overlay {
            base: self,
            view: Cow::Borrowed(&self.image),
        }
    }

    /// How many deep copies of the image this value and its overlays made.
    #[cfg(test)]
    pub(super) fn copies(&self) -> u64 { self.copies.get() }

    fn copy_of(&self, image: &MetadataImage) -> MetadataImage {
        #[cfg(test)]
        self.copies.set(self.copies.get() + 1);
        image.clone()
    }
}

impl Deref for AppliedImage {
    type Target = MetadataImage;

    fn deref(&self) -> &MetadataImage { &self.image }
}

/// A view of an [`AppliedImage`] that copies the image at its first write.
pub(super) struct Overlay<'a> {
    base: &'a AppliedImage,
    view: Cow<'a, MetadataImage>,
}

impl Overlay<'_> {
    /// Applies `record` to the view, copying the image first if no record has
    /// been applied yet.
    pub(super) fn apply(&mut self, record: &MetadataRecord) {
        if let Cow::Borrowed(image) = self.view {
            self.view = Cow::Owned(self.base.copy_of(image));
        }
        self.view.to_mut().apply(record);
    }
}

impl Deref for Overlay<'_> {
    type Target = MetadataImage;

    fn deref(&self) -> &MetadataImage { &self.view }
}
