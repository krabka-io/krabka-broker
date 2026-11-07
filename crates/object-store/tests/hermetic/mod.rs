//! The hermetic object stores the scenarios run against, shared by the
//! suites that illustrate a rule on more than one backend.

use {
    krabka_object_store::ObjectStoreClient,
    object_store::{ObjectStore, local::LocalFileSystem, memory::InMemory},
    std::sync::Arc,
    tempfile::TempDir,
    tokio::runtime::{Builder, Runtime},
};

/// The store backend a scenario writes to.
#[derive(Clone, Copy, Debug, krabka_bdd::StepParam)]
pub enum Backend {
    InMemory,
    LocalFilesystem,
}

/// An open store, the client a broker reaches it through, and the runtime
/// the steps drive it on.
pub struct Hermetic {
    pub runtime: Runtime,
    pub store: Arc<dyn ObjectStore>,
    pub client: ObjectStoreClient,
    /// The root of a local-filesystem store, held so the directory outlives
    /// the scenario.
    pub root: Option<TempDir>,
}

impl Hermetic {
    pub fn open(backend: Backend) -> Self {
        let (store, root): (Arc<dyn ObjectStore>, _) = match backend {
            Backend::InMemory => (Arc::new(InMemory::new()), None),
            Backend::LocalFilesystem => {
                let root = tempfile::tempdir().unwrap();
                let store = LocalFileSystem::new_with_prefix(root.path()).unwrap();
                (Arc::new(store), Some(root))
            }
        };
        Self {
            runtime: Builder::new_current_thread().enable_all().build().unwrap(),
            client: ObjectStoreClient::new(Arc::clone(&store)),
            store,
            root,
        }
    }
}
