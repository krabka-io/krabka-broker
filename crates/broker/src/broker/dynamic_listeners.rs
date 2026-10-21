//! The data-plane listeners a broker runs, and Kafka 4.3.1's
//! `DynamicListenerConfig.reconfigure`, which opens and closes them while the
//! broker runs as this node's dynamic `listeners` changes.
//!
//! Kafka diffs the old and the new listeners by normalised name. A name that
//! is new opens, through `SocketServer.addListeners`: an acceptor bound to the
//! endpoint, with a channel builder made from the config of that moment. A
//! name that is gone closes, through `SocketServer.removeListeners`: its
//! acceptor stops and its connections close. A name that stays is left as it
//! is, even when its host or port changed. A listener that fails to open is
//! logged and left out; the alter that asked for it has already succeeded.
//!
//! [`DataPlaneListeners`] is the set of running listeners, the static ones
//! the startup binds included. The watcher [`spawn_watcher`] starts reads
//! this node's listeners out of each metadata image and makes the set follow
//! them.

use {
    crate::{
        broker::{Broker, accept::accept_loop},
        config::ListenerSpec,
        config_keys::{
            broker_dynamic::stored_dynamic_configs,
            listener_endpoints::{Endpoint, ListenerView, normalised, static_originals},
        },
        error::BrokerError,
        network::auth::SslPrincipalMapper,
        platform::bind_listener,
    },
    krabka_metadata::MetadataImage,
    krabka_security::SaslMechanism,
    std::{
        collections::{BTreeMap, BTreeSet},
        io,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::{Arc, Mutex, MutexGuard, PoisonError},
    },
    tokio::{net::TcpListener, task::JoinHandle},
    tokio_util::sync::CancellationToken,
};

/// One data-plane listener the broker runs.
struct Running {
    spec: ListenerSpec,
    /// Stops this listener's accept loop alone. A child of the broker's
    /// shutdown token, so the broker's shutdown stops it too.
    stop: CancellationToken,
    task: JoinHandle<()>,
}

impl Running {
    fn key(&self) -> String { normalised(&self.spec.name) }
}

#[derive(Default)]
struct State {
    running: Vec<Running>,
    /// The endpoint each listener that failed to open was asked for, by
    /// normalised name. The listener is not tried again until its endpoint
    /// changes or it leaves the config and comes back.
    failed: BTreeMap<String, Endpoint>,
    /// Set by [`DataPlaneListeners::close`]: no listener opens after it.
    closed: bool,
}

/// The data-plane listeners a node with the broker role runs: the static
/// ones the startup binds and the ones a dynamic `listeners` adds.
///
/// Each runs its own accept loop under its own child of the broker's
/// shutdown token, so one closes alone and the broker's shutdown closes all
/// of them.
pub(crate) struct DataPlaneListeners {
    shutdown: CancellationToken,
    state: Mutex<State>,
}

impl DataPlaneListeners {
    /// An empty set whose listeners stop when `shutdown` is cancelled.
    pub(crate) fn new(shutdown: CancellationToken) -> Self {
        Self {
            shutdown,
            state: Mutex::new(State::default()),
        }
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The specs of the running listeners.
    pub(crate) fn specs(&self) -> Vec<ListenerSpec> {
        self.state()
            .running
            .iter()
            .map(|running| running.spec.clone())
            .collect()
    }

    /// The normalised names of the running listeners.
    fn names(&self) -> Vec<String> { self.state().running.iter().map(Running::key).collect() }

    /// Kafka's `ConnectionQuotas.protectedListener`, evaluated at each
    /// accept: the inter-broker listener, while the broker runs more than one
    /// listener, is exempt from the broker-wide connection cap and rate.
    pub(crate) fn is_protected(&self, listener: &str, inter_broker_listener: &str) -> bool {
        listener.eq_ignore_ascii_case(inter_broker_listener) && self.state().running.len() > 1
    }

    /// Run the accept loop of `spec` on `listener`, unless the set is
    /// closed, in which case the listener is dropped.
    pub(super) fn start(broker: &Arc<Broker>, spec: ListenerSpec, listener: TcpListener) {
        let listeners = &broker.data_plane;
        let mut state = listeners.state();
        if state.closed {
            return;
        }
        let stop = listeners.shutdown.child_token();
        let task = tokio::spawn(accept_loop(
            Arc::clone(broker),
            listener,
            spec.clone(),
            stop.clone(),
        ));
        state.failed.remove(&normalised(&spec.name));
        state.running.push(Running { spec, stop, task });
    }

    /// Stop the listeners named in `names` and hand back their accept loops
    /// to await.
    fn stop(&self, names: &[String]) -> Vec<JoinHandle<()>> {
        let mut state = self.state();
        let (stopped, kept) = std::mem::take(&mut state.running)
            .into_iter()
            .partition::<Vec<_>, _>(|running| names.contains(&running.key()));
        state.running = kept;
        stopped
            .into_iter()
            .map(|running| {
                running.stop.cancel();
                running.task
            })
            .collect()
    }

    /// Record that the listener `endpoint` names failed to open.
    fn record_failure(&self, endpoint: Endpoint) {
        self.state().failed.insert(endpoint.name.clone(), endpoint);
    }

    /// Close the set: no listener opens after this, and the accept loops of
    /// the running ones are handed back to await or abort. The caller stops
    /// them, by cancelling the broker's shutdown token.
    pub(super) fn close(&self) -> Vec<JoinHandle<()>> {
        let mut state = self.state();
        state.closed = true;
        state.failed.clear();
        state
            .running
            .drain(..)
            .map(|running| running.task)
            .collect()
    }
}

/// What a change of this node's listeners does to the running set.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct ListenerChanges {
    /// The endpoints to open: each named in the config, not running, and not
    /// a failure of the same endpoint.
    pub(super) start: Vec<Endpoint>,
    /// The normalised names of the running listeners the config no longer
    /// names.
    pub(super) stop: Vec<String>,
}

/// Kafka's diff of the old and the new listeners, by normalised name, made
/// against what runs: a name the config adds opens, a name it drops closes,
/// and a name it keeps stays as it runs, whatever its endpoint now says.
///
/// A listener that `failed` to open on an endpoint is not tried again on the
/// same endpoint.
pub(super) fn listener_changes(
    running: &[String],
    failed: &BTreeMap<String, Endpoint>,
    desired: &[Endpoint],
) -> ListenerChanges {
    let wanted: BTreeSet<&str> = desired.iter().map(|ep| ep.name.as_str()).collect();
    ListenerChanges {
        start: desired
            .iter()
            .filter(|ep| !running.contains(&ep.name) && failed.get(&ep.name) != Some(*ep))
            .cloned()
            .collect(),
        stop: running
            .iter()
            .filter(|name| !wanted.contains(name.as_str()))
            .cloned()
            .collect(),
    }
}

/// The address a listener on `endpoint` binds: the wildcard `0.0.0.0` for an
/// empty host, an IP literal as it is, and the first address a hostname
/// resolves to.
///
/// # Errors
/// A port outside `u16`, or a hostname that does not resolve.
pub(super) async fn bind_address(endpoint: &Endpoint) -> io::Result<SocketAddr> {
    let port = u16::try_from(endpoint.port).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("port {} is out of range", endpoint.port),
        )
    })?;
    let Some(host) = endpoint.host.as_deref() else {
        return Ok(SocketAddr::from((Ipv4Addr::UNSPECIFIED, port)));
    };
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(SocketAddr::from((ip, port)));
    }
    tokio::net::lookup_host((host, port))
        .await?
        .next()
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("host {host} has no address"),
            )
        })
}

/// What a listener that opens while the broker runs is built from.
#[derive(Debug, Clone, bon::Builder)]
pub(super) struct AddedListener<'a> {
    pub(super) endpoint: &'a Endpoint,
    /// The address [`bind_address`] gave for the endpoint.
    pub(super) bind_addr: SocketAddr,
    /// Kafka's `saslEnabledMechanisms(listenerName)` as of the change.
    pub(super) sasl_mechanisms: Vec<SaslMechanism>,
    pub(super) principal_mapper: SslPrincipalMapper,
}

impl AddedListener<'_> {
    /// The listener's spec. It carries no TLS material of its own: a TLS
    /// listener starts from the broker-wide `tls_config`, under its dynamic
    /// `listener.name.<listener>.ssl.*` keys. Its advertised address is its
    /// endpoint, which is not registered with the controller, as Kafka
    /// registers no listener it adds.
    pub(super) fn spec(self) -> ListenerSpec {
        let advertised = match self.endpoint.host.as_deref() {
            Some(host) if host.contains(':') => format!("[{host}]:{}", self.endpoint.port),
            Some(host) => format!("{host}:{}", self.endpoint.port),
            None => self.bind_addr.to_string(),
        };
        ListenerSpec {
            name: self.endpoint.name.clone(),
            bind_addr: self.bind_addr,
            advertised,
            protocol: self.endpoint.protocol,
            tls_config: None,
            sasl_mechanisms: Some(self.sasl_mechanisms),
            principal_mapper: self.principal_mapper,
        }
    }
}

/// Bind the address of the added listener `spec`. A failure names the
/// listener, its address and the dynamic `listeners` config that asked for it.
async fn bind_added(spec: &ListenerSpec) -> io::Result<TcpListener> {
    bind_listener(spec.bind_addr).await.map_err(|error| {
        io::Error::new(
            error.kind(),
            BrokerError::listener_bind(&spec.name, spec.bind_addr, "listeners")(error),
        )
    })
}

/// Open the listener on `endpoint`, as Kafka's `SocketServer.addListeners`
/// does: build its spec from `view`, check that the broker can serve it,
/// bind it and start its accept loop. A failure is logged and recorded, and
/// the listener stays closed.
async fn open(broker: &Arc<Broker>, view: &ListenerView, endpoint: Endpoint) {
    let opened = async {
        let bind_addr = bind_address(&endpoint).await?;
        let spec = AddedListener::builder()
            .endpoint(&endpoint)
            .bind_addr(bind_addr)
            .sasl_mechanisms(view.sasl_enabled_mechanisms(&endpoint.name))
            .principal_mapper(broker.config.tls_principal_mapper.clone())
            .build()
            .spec();
        broker
            .config
            .validate_added_listener(&spec)
            .map_err(io::Error::other)?;
        let listener = bind_added(&spec).await?;
        Ok::<_, io::Error>((spec, listener))
    }
    .await;
    match opened {
        Ok((spec, listener)) => {
            tracing::info!(name = %spec.name, addr = %spec.bind_addr, "listener added");
            DataPlaneListeners::start(broker, spec, listener);
        }
        Err(error) => {
            tracing::error!(
                name = %endpoint.name,
                %error,
                "failed to add the listener; it stays closed"
            );
            broker.data_plane.record_failure(endpoint);
        }
    }
}

/// Make the running listeners follow the listeners `image` gives this node.
/// A config whose listeners do not parse changes nothing.
async fn apply(
    broker: &Arc<Broker>,
    static_config: &BTreeMap<String, String>,
    image: &MetadataImage,
    shutdown: &CancellationToken,
) {
    let view = ListenerView::new(
        static_config,
        image.default_broker_config(),
        &stored_dynamic_configs(image, broker.config.node_id),
    );
    let desired = match view.data_plane_listeners() {
        Ok(desired) => desired,
        Err(error) => {
            tracing::warn!(%error, "the listeners do not parse; the running listeners stay");
            return;
        }
    };
    let changes = {
        let mut state = broker.data_plane.state();
        state
            .failed
            .retain(|name, _| desired.iter().any(|ep| ep.name == *name));
        let running: Vec<String> = state.running.iter().map(Running::key).collect();
        listener_changes(&running, &state.failed, &desired)
    };
    // Removed before added, as Kafka removes first: a port the removed
    // listener held is free for the added one.
    for task in broker.data_plane.stop(&changes.stop) {
        drop(task.await);
    }
    for name in &changes.stop {
        tracing::info!(%name, "listener removed");
    }
    for endpoint in changes.start {
        if shutdown.is_cancelled() {
            return;
        }
        open(broker, &view, endpoint).await;
    }
}

/// Watch the metadata image and make the running data-plane listeners follow
/// this node's listeners until `shutdown`. The current image applies first,
/// so a dynamic `listeners` stored before a restart opens at startup.
///
/// `None` on a node without the broker role, which runs no data-plane
/// listener, and on a node whose static listeners the config view does not
/// read back as running: the view could not tell its own listeners apart
/// from a change, so the node keeps its static listeners and opens no other.
pub(super) fn spawn_watcher(
    broker: &Arc<Broker>,
    shutdown: CancellationToken,
) -> Option<JoinHandle<()>> {
    if !broker.config.is_broker() {
        return None;
    }
    let static_config = static_originals(&broker.config);
    let mut running = broker.data_plane.names();
    running.sort_unstable();
    match ListenerView::new(&static_config, None, &BTreeMap::new()).data_plane_listeners() {
        Ok(listeners) => {
            let mut names: Vec<String> = listeners.into_iter().map(|ep| ep.name).collect();
            names.sort_unstable();
            if names != running {
                tracing::warn!(
                    ?names,
                    ?running,
                    "the static listeners read back as others; dynamic listeners are off"
                );
                return None;
            }
        }
        Err(error) => {
            tracing::warn!(%error, "the static listeners do not parse; dynamic listeners are off");
            return None;
        }
    }
    let broker = Arc::clone(broker);
    let mut images = broker.controller.watch_image();
    Some(tokio::spawn(async move {
        loop {
            let image = Arc::clone(&images.borrow_and_update());
            tokio::select! {
                () = shutdown.cancelled() => break,
                () = apply(&broker, &static_config, &image, &shutdown) => {}
            }
            tokio::select! {
                () = shutdown.cancelled() => break,
                changed = images.changed() => {
                    if changed.is_err() {
                        break;
                    }
                }
            }
        }
    }))
}

#[cfg(test)]
mod tests;
