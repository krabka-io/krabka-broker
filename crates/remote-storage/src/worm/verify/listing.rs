//! What the object store actually holds, grouped the way a partition sits in
//! it.
//!
//! The verifier never trusts a manifest about the archive's shape, so it lists
//! the store once and compares every later claim against that listing. This
//! module produces the listing and answers the one question the listing alone
//! settles: which objects no manifest accounts for.

use {
    super::manifest_read::KeyedManifest,
    crate::worm::{error::WormError, manifest::MANIFEST_SUFFIX},
    futures_util::TryStreamExt as _,
    krabka_object_store::normalize_prefix,
    object_store::{ObjectStore, path::Path},
    std::{
        collections::{BTreeMap, BTreeSet},
        iter,
        sync::Arc,
    },
};

/// One directory's objects: full object-store key to size in bytes.
pub(super) type DirListing = BTreeMap<String, u64>;

/// The component that marks a backup capture's namespace.
const CAPTURE_NAMESPACE: &str = "restore-inputs";

/// What the store holds below a request's prefix.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct ArchiveListing {
    /// Each directory, named below the trimmed prefix, to its objects.
    pub(super) dirs: BTreeMap<String, DirListing>,
    /// Stored keys inside a backup capture's namespace.
    pub(super) captured: BTreeSet<String>,
}

/// One listed object: where it sits below the root it was listed under.
struct Listed {
    relative: String,
    size: u64,
}

/// Lists the archive and groups it by the directory each key sits in.
///
/// A broker before `krabka.version` 3 writes the prefix as configured, stray
/// whitespace included, and one at level 3 writes it trimmed, so an archive
/// can hold one partition's chain under both spellings. The listing reads the
/// trimmed root and, when the prefix as given names a different path, that
/// root too. Each object is grouped by its directory below the root it was
/// listed under, and the directory is named below the trimmed root, so both
/// halves of a chain meet in one directory. Each entry keeps the object's full
/// stored key, which is what a manifest names. A prefix with no stray
/// characters has one root, and lists exactly as it always has.
pub(super) async fn list_archive(
    store: &Arc<dyn ObjectStore>,
    prefix: Option<&str>,
) -> Result<ArchiveListing, WormError> {
    let trimmed = Path::from(normalize_prefix(prefix).unwrap_or_default());
    let given = prefix.map_or_else(Path::default, Path::from);
    // The as-given root goes last: when the trimmed one is the whole store, its
    // listing holds the as-given keys too, and their place below the narrower
    // root wins.
    let mut listed: BTreeMap<String, Listed> = BTreeMap::new();
    for root in iter::once(&trimmed).chain((given != trimmed).then_some(&given)) {
        list_root(store, root, &mut listed).await?;
    }
    let mut listing = ArchiveListing::default();
    for (key, Listed { relative, size }) in listed {
        let dir = relative.rsplit_once('/').map_or("", |(dir, _)| dir);
        let dir = match (trimmed.as_ref(), dir) {
            (root, "") => root.to_owned(),
            ("", dir) => dir.to_owned(),
            (root, dir) => format!("{root}/{dir}"),
        };
        if relative
            .split('/')
            .any(|component| component == CAPTURE_NAMESPACE)
        {
            listing.captured.insert(key.clone());
        }
        listing.dirs.entry(dir).or_default().insert(key, size);
    }
    Ok(listing)
}

/// Lists every object below `root`, the whole store when it is empty.
async fn list_root(
    store: &Arc<dyn ObjectStore>,
    root: &Path,
    listed: &mut BTreeMap<String, Listed>,
) -> Result<(), WormError> {
    let scope = (!root.as_ref().is_empty()).then_some(root);
    let mut stream = store.list(scope);
    loop {
        let next = stream.try_next().await.map_err(|e| {
            WormError::Archive(format!("cannot list the archive under `{root}`: {e}"))
        })?;
        let Some(meta) = next else { break };
        let key = meta.location.to_string();
        let relative = key
            .strip_prefix(root.as_ref())
            .and_then(|rest| rest.strip_prefix('/'))
            .unwrap_or(&key)
            .to_owned();
        listed.insert(key, Listed {
            relative,
            size: meta.size,
        });
    }
    Ok(())
}

/// Objects in the directory that no manifest names, sorted by key.
pub(super) fn orphans(listing: &DirListing, decoded: &[KeyedManifest]) -> Vec<String> {
    let referenced: BTreeSet<&str> = decoded
        .iter()
        .flat_map(|KeyedManifest { manifest, .. }| {
            manifest
                .body
                .objects
                .iter()
                .map(|object| object.key.as_str())
        })
        .collect();
    listing
        .keys()
        .filter(|key| !key.ends_with(MANIFEST_SUFFIX) && !referenced.contains(key.as_str()))
        .cloned()
        .collect()
}
