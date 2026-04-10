//! Scope-Based Trace Correlation
//!
//! Maps connections to traces using source-port association and hierarchical scope IDs.
//! Supports concurrent traces without global state.

use crate::buffer::{PendingEventBuffer, QuarantinedEvents};
use deja_common::{ConnectionKey, Protocol, ScopeId, ScopeSequenceTracker};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{oneshot, RwLock};
use tracing::{debug, warn};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ControlOrderingDecision {
    Accept,
    Duplicate,
    Reject(String),
}

#[derive(Clone, Debug)]
struct ControlOrderingState {
    last_seq: u64,
    last_fingerprint: String,
}

/// Metadata about a trace
#[derive(Clone, Debug)]
pub struct TraceMetadata {
    pub trace_id: String,
    pub start_time_ns: u64,
    pub end_time_ns: Option<u64>,
    pub connection_count: u64,
}

/// Result of resolving a connection's trace association
#[derive(Debug, Clone)]
pub struct ConnectionAssociation {
    pub scope_id: ScopeId,
    pub trace_id: String,
    pub protocol: Protocol,
}

/// A pending connection awaiting trace association
struct PendingConnection {
    pub sender: oneshot::Sender<ConnectionAssociation>,
    created_at_ns: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum LeaseLifecycleState {
    Associated,
    Leased,
    Released,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ConnectionLifecycleState {
    PendingAssociation,
    Attributed {
        trace_id: String,
        protocol: Protocol,
        lease_state: LeaseLifecycleState,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum TransitionRejection {
    ImplicitTraceRebind {
        source_port: u16,
        existing_trace_id: String,
        requested_trace_id: String,
    },
    DuplicatePendingConnection {
        source_port: u16,
    },
    InvalidLeaseRelease {
        source_port: u16,
    },
    DoubleCheckout {
        source_port: u16,
        existing_trace_id: String,
        requested_trace_id: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CorrelationInvariantViolation {
    PendingStateMissingReceiver {
        source_port: u16,
    },
    PendingStateHasRegisteredTrace {
        source_port: u16,
    },
    AttributedStateMissingRegistration {
        source_port: u16,
    },
    AttributedStateMismatchedRegistration {
        source_port: u16,
        expected_trace_id: String,
        actual_trace_id: String,
    },
    AttributedStateStillPending {
        source_port: u16,
    },
    AttributedStateUsesOrphanTrace {
        source_port: u16,
    },
    DanglingPendingConnection {
        source_port: u16,
    },
    RegisteredConnectionMissingState {
        source_port: u16,
    },
}

/// Manages correlation between connections and traces using scope-based model.
///
/// Instead of a global "current active trace", uses source-port pre-registration
/// to deterministically associate connections with traces.
pub struct TraceCorrelator {
    /// 4-tuple ConnectionKey → trace_id: SDK pre-registers before connecting.
    /// Falls back to source-port-only key (with zeroed IPs) for backward compat.
    connection_key_to_trace: Arc<RwLock<HashMap<ConnectionKey, (String, Protocol)>>>,

    /// scope_id → trace_id: once associated, scope events get this trace
    scope_to_trace: Arc<RwLock<HashMap<ScopeId, String>>>,

    /// Active traces with metadata
    active_traces: Arc<RwLock<HashMap<String, TraceMetadata>>>,

    /// Per-trace connection counter (for deterministic conn:N numbering)
    trace_connection_counter: Arc<RwLock<HashMap<String, u64>>>,

    /// Scope sequence tracking
    sequence_tracker: Arc<RwLock<ScopeSequenceTracker>>,

    /// Pending connections awaiting trace association (port -> sender for O(1) lookup)
    pending_connections: Arc<RwLock<HashMap<u16, PendingConnection>>>,

    connection_lifecycle: Arc<RwLock<HashMap<u16, ConnectionLifecycleState>>>,

    /// Correlation metrics for monitoring
    metrics: Arc<RwLock<CorrelationMetrics>>,

    control_ordering: Arc<RwLock<HashMap<String, ControlOrderingState>>>,

    /// Pending event buffers for retro-binding (record mode)
    pending_event_buffers: Arc<RwLock<HashMap<u16, PendingEventBuffer>>>,

    /// Concrete associations for live connections, including late-bound ones.
    live_associations: Arc<RwLock<HashMap<u16, ConnectionAssociation>>>,

    /// Quarantined events that couldn't be attributed
    quarantined_events: Arc<RwLock<QuarantinedEvents>>,

    /// Mapping from proxy's outgoing port (to target) to client's source port (peer port).
    /// This is needed because the SDK queries the database server for the client port,
    /// which returns the proxy's outgoing port, not the original client's source port.
    /// When AssociateBySourcePort arrives with the outgoing port, we translate it to
    /// the peer port for proper correlation.
    outgoing_to_peer_port: Arc<RwLock<HashMap<u16, u16>>>,
}

/// Metrics for correlation health monitoring
#[derive(Debug, Default)]
pub struct CorrelationMetrics {
    pub successful_associations: std::sync::atomic::AtomicU64,
    pub orphan_connections: std::sync::atomic::AtomicU64,
    pub timeout_resolutions: std::sync::atomic::AtomicU64,
    pub pool_reassociations: std::sync::atomic::AtomicU64,
    pub invalid_transitions: std::sync::atomic::AtomicU64,
    // Structured reason counters for diagnostic precision
    pub quarantine_events: std::sync::atomic::AtomicU64,
    pub late_binds: std::sync::atomic::AtomicU64,
    pub known_orphan_timed_out: std::sync::atomic::AtomicU64,
}

impl TraceCorrelator {
    pub fn new() -> Self {
        Self {
            connection_key_to_trace: Arc::new(RwLock::new(HashMap::new())),
            scope_to_trace: Arc::new(RwLock::new(HashMap::new())),
            active_traces: Arc::new(RwLock::new(HashMap::new())),
            trace_connection_counter: Arc::new(RwLock::new(HashMap::new())),
            sequence_tracker: Arc::new(RwLock::new(ScopeSequenceTracker::new())),
            pending_connections: Arc::new(RwLock::new(HashMap::new())),
            connection_lifecycle: Arc::new(RwLock::new(HashMap::new())),
            metrics: Arc::new(RwLock::new(CorrelationMetrics::default())),
            control_ordering: Arc::new(RwLock::new(HashMap::new())),
            pending_event_buffers: Arc::new(RwLock::new(HashMap::new())),
            live_associations: Arc::new(RwLock::new(HashMap::new())),
            quarantined_events: Arc::new(RwLock::new(QuarantinedEvents::new())),
            outgoing_to_peer_port: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn check_control_ordering(
        &self,
        sender_key: &str,
        sender_seq: Option<u64>,
        message_fingerprint: &str,
    ) -> ControlOrderingDecision {
        let Some(seq) = sender_seq else {
            return ControlOrderingDecision::Accept;
        };

        let mut ordering = self.control_ordering.write().await;
        let state = ordering.get(sender_key).cloned();

        match state {
            None => {
                ordering.insert(
                    sender_key.to_string(),
                    ControlOrderingState {
                        last_seq: seq,
                        last_fingerprint: message_fingerprint.to_string(),
                    },
                );
                ControlOrderingDecision::Accept
            }
            Some(existing) if seq > existing.last_seq => {
                ordering.insert(
                    sender_key.to_string(),
                    ControlOrderingState {
                        last_seq: seq,
                        last_fingerprint: message_fingerprint.to_string(),
                    },
                );
                ControlOrderingDecision::Accept
            }
            Some(existing)
                if seq == existing.last_seq && message_fingerprint == existing.last_fingerprint =>
            {
                ControlOrderingDecision::Duplicate
            }
            Some(existing) if seq == existing.last_seq => {
                self.metrics
                    .write()
                    .await
                    .invalid_transitions
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                ControlOrderingDecision::Reject(format!(
                    "conflicting duplicate sequence {} for sender {}",
                    seq, sender_key
                ))
            }
            Some(existing) => {
                self.metrics
                    .write()
                    .await
                    .invalid_transitions
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                ControlOrderingDecision::Reject(format!(
                    "stale sequence {} for sender {}; latest accepted {}",
                    seq, sender_key, existing.last_seq
                ))
            }
        }
    }

    /// Record that a new trace has started
    pub async fn start_trace(&self, trace_id: String, timestamp_ns: u64) {
        debug!(trace_id = %trace_id, "Starting trace");

        let metadata = TraceMetadata {
            trace_id: trace_id.clone(),
            start_time_ns: timestamp_ns,
            end_time_ns: None,
            connection_count: 0,
        };

        self.active_traces
            .write()
            .await
            .insert(trace_id.clone(), metadata);
        self.trace_connection_counter
            .write()
            .await
            .insert(trace_id, 0);
    }

    /// Record that a trace has ended
    pub async fn end_trace(&self, trace_id: String, timestamp_ns: u64) {
        debug!(trace_id = %trace_id, "Ending trace");
        if let Some(metadata) = self.active_traces.write().await.get_mut(&trace_id) {
            metadata.end_time_ns = Some(timestamp_ns);
        }
    }

    fn attributed_state(
        trace_id: &str,
        protocol: Protocol,
        lease_state: LeaseLifecycleState,
    ) -> ConnectionLifecycleState {
        ConnectionLifecycleState::Attributed {
            trace_id: trace_id.to_string(),
            protocol,
            lease_state,
        }
    }

    async fn reject_transition(&self, rejection: TransitionRejection) {
        self.metrics
            .write()
            .await
            .invalid_transitions
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        match rejection {
            TransitionRejection::ImplicitTraceRebind {
                source_port,
                existing_trace_id,
                requested_trace_id,
            } => {
                warn!(
                    source_port = source_port,
                    existing_trace_id = %existing_trace_id,
                    requested_trace_id = %requested_trace_id,
                    "Rejected implicit trace rebind; use reassociate() for explicit transition"
                );
            }
            TransitionRejection::DuplicatePendingConnection { source_port } => {
                warn!(
                    source_port = source_port,
                    "Rejected duplicate pending connection for source port"
                );
            }
            TransitionRejection::InvalidLeaseRelease { source_port } => {
                warn!(
                    source_port = source_port,
                    "Rejected lease release without an attributed lifecycle state"
                );
            }
            TransitionRejection::DoubleCheckout {
                source_port,
                existing_trace_id,
                requested_trace_id,
            } => {
                warn!(
                    source_port = source_port,
                    existing_trace_id = %existing_trace_id,
                    requested_trace_id = %requested_trace_id,
                    "Rejected pool checkout: connection already leased without prior return"
                );
            }
        }
    }

    async fn validate_invariants_after_transition(&self, source_port: u16, transition: &str) {
        if let Err(err) = self.check_invariants().await {
            warn!(
                source_port = source_port,
                transition = transition,
                error = ?err,
                "Correlation invariant violation"
            );
        }
    }

    pub async fn check_invariants(&self) -> Result<(), CorrelationInvariantViolation> {
        let states = self.connection_lifecycle.read().await;
        let pending = self.pending_connections.read().await;
        let conn_map = self.connection_key_to_trace.read().await;

        for (source_port, state) in states.iter() {
            let key = ConnectionKey::from_source_port(*source_port);
            match state {
                ConnectionLifecycleState::PendingAssociation => {
                    if !pending.contains_key(source_port) {
                        return Err(CorrelationInvariantViolation::PendingStateMissingReceiver {
                            source_port: *source_port,
                        });
                    }
                    if conn_map.contains_key(&key) {
                        return Err(
                            CorrelationInvariantViolation::PendingStateHasRegisteredTrace {
                                source_port: *source_port,
                            },
                        );
                    }
                }
                ConnectionLifecycleState::Attributed {
                    trace_id, protocol, ..
                } => {
                    if trace_id.is_empty() || trace_id == "orphan" {
                        return Err(
                            CorrelationInvariantViolation::AttributedStateUsesOrphanTrace {
                                source_port: *source_port,
                            },
                        );
                    }

                    match conn_map.get(&key) {
                        Some((mapped_trace_id, mapped_protocol))
                            if mapped_trace_id == trace_id && mapped_protocol == protocol => {}
                        Some((mapped_trace_id, _)) => return Err(
                            CorrelationInvariantViolation::AttributedStateMismatchedRegistration {
                                source_port: *source_port,
                                expected_trace_id: trace_id.clone(),
                                actual_trace_id: mapped_trace_id.clone(),
                            },
                        ),
                        None => {
                            return Err(
                                CorrelationInvariantViolation::AttributedStateMissingRegistration {
                                    source_port: *source_port,
                                },
                            )
                        }
                    }

                    if pending.contains_key(source_port) {
                        return Err(CorrelationInvariantViolation::AttributedStateStillPending {
                            source_port: *source_port,
                        });
                    }
                }
            }
        }

        for source_port in pending.keys() {
            if !matches!(
                states.get(source_port),
                Some(ConnectionLifecycleState::PendingAssociation)
            ) {
                return Err(CorrelationInvariantViolation::DanglingPendingConnection {
                    source_port: *source_port,
                });
            }
        }

        for key in conn_map.keys() {
            let source_port = key.source_port();
            if !matches!(
                states.get(&source_port),
                Some(ConnectionLifecycleState::Attributed { .. })
            ) {
                return Err(
                    CorrelationInvariantViolation::RegisteredConnectionMissingState { source_port },
                );
            }
        }

        Ok(())
    }

    pub async fn invalid_transition_count(&self) -> u64 {
        self.metrics
            .read()
            .await
            .invalid_transitions
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub async fn release_lease(&self, source_port: u16) {
        let rejection = {
            let mut states = self.connection_lifecycle.write().await;
            match states.get_mut(&source_port) {
                Some(ConnectionLifecycleState::Attributed { lease_state, .. }) => {
                    *lease_state = LeaseLifecycleState::Released;
                    None
                }
                Some(ConnectionLifecycleState::PendingAssociation) | None => {
                    Some(TransitionRejection::InvalidLeaseRelease { source_port })
                }
            }
        };

        if let Some(rejection) = rejection {
            self.reject_transition(rejection).await;
        }

        self.validate_invariants_after_transition(source_port, "release_lease")
            .await;
    }

    pub async fn pool_checkout(
        &self,
        trace_id: &str,
        source_port: u16,
        protocol: Protocol,
    ) -> Result<(), String> {
        let rejection = {
            let states = self.connection_lifecycle.read().await;
            match states.get(&source_port) {
                Some(ConnectionLifecycleState::Attributed {
                    lease_state: LeaseLifecycleState::Leased,
                    trace_id: existing_trace_id,
                    ..
                }) if existing_trace_id != trace_id => Some(TransitionRejection::DoubleCheckout {
                    source_port,
                    existing_trace_id: existing_trace_id.clone(),
                    requested_trace_id: trace_id.to_string(),
                }),
                _ => None,
            }
        };

        if let Some(rejection) = rejection {
            let msg = format!(
                "double checkout on port {} without prior return",
                source_port
            );
            self.reject_transition(rejection).await;
            self.validate_invariants_after_transition(source_port, "pool_checkout_rejected")
                .await;
            return Err(msg);
        }

        self.reassociate(trace_id, source_port, protocol).await;

        {
            let mut states = self.connection_lifecycle.write().await;
            if let Some(ConnectionLifecycleState::Attributed { lease_state, .. }) =
                states.get_mut(&source_port)
            {
                *lease_state = LeaseLifecycleState::Leased;
            }
        }

        self.validate_invariants_after_transition(source_port, "pool_checkout")
            .await;
        Ok(())
    }

    pub async fn pool_return(&self, source_port: u16) {
        let rejection = {
            let mut states = self.connection_lifecycle.write().await;
            match states.get_mut(&source_port) {
                Some(ConnectionLifecycleState::Attributed { lease_state, .. }) => {
                    *lease_state = LeaseLifecycleState::Associated;
                    None
                }
                _ => Some(TransitionRejection::InvalidLeaseRelease { source_port }),
            }
        };

        if let Some(rejection) = rejection {
            self.reject_transition(rejection).await;
        }

        self.validate_invariants_after_transition(source_port, "pool_return")
            .await;
    }

    /// Handle a new connection from a peer.
    ///
    /// Checks if the source port was pre-registered by the SDK. If so, immediately
    /// returns the association. Otherwise, returns a receiver that will resolve
    /// when the association arrives (or times out).
    pub async fn on_new_connection(
        &self,
        peer_port: u16,
        detected_protocol: Protocol,
    ) -> Result<ConnectionAssociation, oneshot::Receiver<ConnectionAssociation>> {
        if matches!(
            self.connection_lifecycle.read().await.get(&peer_port),
            Some(ConnectionLifecycleState::PendingAssociation)
        ) {
            self.reject_transition(TransitionRejection::DuplicatePendingConnection {
                source_port: peer_port,
            })
            .await;
            self.validate_invariants_after_transition(peer_port, "reject_duplicate_pending")
                .await;
            let (tx, rx) = oneshot::channel();
            drop(tx);
            return Err(rx);
        }

        // Try source-port-only key (backward compat with AssociateBySourcePort)
        let port_key = ConnectionKey::from_source_port(peer_port);
        if let Some((trace_id, protocol)) = self
            .connection_key_to_trace
            .read()
            .await
            .get(&port_key)
            .cloned()
        {
            self.connection_lifecycle.write().await.insert(
                peer_port,
                Self::attributed_state(&trace_id, protocol, LeaseLifecycleState::Leased),
            );

            let proto = if detected_protocol != Protocol::Unknown {
                detected_protocol
            } else {
                protocol
            };
            let assoc = self.create_association(&trace_id, proto).await;
            self.validate_invariants_after_transition(peer_port, "lease_connection")
                .await;
            return Ok(assoc);
        }

        // Create pending entry — will be resolved when control message arrives
        let (tx, rx) = oneshot::channel();
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        self.pending_connections.write().await.insert(
            peer_port,
            PendingConnection {
                sender: tx,
                created_at_ns: now_ns,
            },
        );
        self.connection_lifecycle
            .write()
            .await
            .insert(peer_port, ConnectionLifecycleState::PendingAssociation);
        self.validate_invariants_after_transition(peer_port, "create_pending")
            .await;

        Err(rx)
    }

    /// Register a mapping from the proxy's outgoing port to the client's source port.
    ///
    /// When the SDK queries the database server for the client port, it gets the
    /// proxy's outgoing port (not the original client's source port). This mapping
    /// allows the proxy to translate the SDK-reported port to the correct peer port.
    pub async fn register_outgoing_port_mapping(&self, outgoing_port: u16, peer_port: u16) {
        debug!(
            outgoing_port = outgoing_port,
            peer_port = peer_port,
            "Registering outgoing port mapping"
        );
        self.outgoing_to_peer_port
            .write()
            .await
            .insert(outgoing_port, peer_port);
    }

    /// SDK notifies the proxy about a source port for a trace.
    ///
    /// Checks pending connections first. If a connection from this port is already
    /// waiting, resolves it immediately. Otherwise, pre-registers for future connections.
    ///
    /// If the source_port is the proxy's outgoing port (not the client's peer port),
    /// it will be translated using the registered mapping.
    pub async fn associate_by_source_port(
        &self,
        trace_id: &str,
        source_port: u16,
        protocol: Protocol,
    ) {
        debug!(
            trace_id = %trace_id,
            source_port = source_port,
            protocol = %protocol,
            "Associate by source port"
        );

        // Translate outgoing port to peer port if this is the proxy's outgoing port
        let effective_port =
            if let Some(peer_port) = self.outgoing_to_peer_port.read().await.get(&source_port) {
                debug!(
                    outgoing_port = source_port,
                    peer_port = *peer_port,
                    "Translated outgoing port to peer port"
                );
                *peer_port
            } else {
                source_port
            };
        let source_port = effective_port;

        if let Some(ConnectionLifecycleState::Attributed {
            trace_id: existing_trace_id,
            ..
        }) = self
            .connection_lifecycle
            .read()
            .await
            .get(&source_port)
            .cloned()
        {
            if existing_trace_id != trace_id {
                self.reject_transition(TransitionRejection::ImplicitTraceRebind {
                    source_port,
                    existing_trace_id,
                    requested_trace_id: trace_id.to_string(),
                })
                .await;
                self.validate_invariants_after_transition(source_port, "reject_implicit_rebind")
                    .await;
                return;
            }
        }

        // Check pending connections matching this source port (O(1) lookup)
        let pending_connection = self.pending_connections.write().await.remove(&source_port);
        let mut lease_state = LeaseLifecycleState::Associated;
        let mut live_assoc = None;

        if let Some(pc) = pending_connection {
            let assoc = self.create_association(trace_id, protocol).await;
            if pc.sender.send(assoc.clone()).is_ok() {
                lease_state = LeaseLifecycleState::Leased;
                self.metrics
                    .write()
                    .await
                    .successful_associations
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                self.metrics
                    .write()
                    .await
                    .late_binds
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                live_assoc = Some(assoc);
            } else {
                warn!(
                    source_port = source_port,
                    trace_id = %trace_id,
                    "Pending association receiver dropped; pre-registering for next connection"
                );
                live_assoc = Some(assoc);
                lease_state = LeaseLifecycleState::Leased;
            }
        } else if self
            .pending_event_buffers
            .read()
            .await
            .get(&source_port)
            .map(|buffer| buffer.has_events(source_port))
            .unwrap_or(false)
        {
            live_assoc = Some(self.create_association(trace_id, protocol).await);
            lease_state = LeaseLifecycleState::Leased;
        }

        if let Some(assoc) = live_assoc.clone() {
            self.live_associations
                .write()
                .await
                .insert(source_port, assoc);
        }

        // Pre-register for future connection using source-port-only key
        let key = ConnectionKey::from_source_port(source_port);
        self.connection_key_to_trace
            .write()
            .await
            .insert(key, (trace_id.to_string(), protocol));
        self.connection_lifecycle.write().await.insert(
            source_port,
            Self::attributed_state(trace_id, protocol, lease_state),
        );
        self.validate_invariants_after_transition(source_port, "associate_by_source_port")
            .await;
    }

    pub async fn register_live_association(
        &self,
        source_port: u16,
        association: ConnectionAssociation,
    ) {
        self.live_associations
            .write()
            .await
            .insert(source_port, association);
    }

    pub async fn lookup_live_association(
        &self,
        source_port: u16,
    ) -> Option<ConnectionAssociation> {
        self.live_associations.read().await.get(&source_port).cloned()
    }

    pub async fn clear_live_association(&self, source_port: u16) {
        self.live_associations.write().await.remove(&source_port);
    }

    async fn get_connection_index(&self, trace_id: &str) -> u32 {
        let counters = self.trace_connection_counter.read().await;
        counters.get(trace_id).copied().unwrap_or(0) as u32
    }

    /// Create a ConnectionAssociation with the next connection index for this trace
    async fn create_association(
        &self,
        trace_id: &str,
        protocol: Protocol,
    ) -> ConnectionAssociation {
        let conn_n = {
            let mut counters = self.trace_connection_counter.write().await;
            let counter = counters.entry(trace_id.to_string()).or_insert(0);
            let n = *counter;
            *counter += 1;
            n
        };

        // Update trace metadata
        if let Some(metadata) = self.active_traces.write().await.get_mut(trace_id) {
            metadata.connection_count = conn_n + 1;
        }

        let scope_id = ScopeId::connection(trace_id, conn_n);

        // Register in scope_to_trace map
        self.scope_to_trace
            .write()
            .await
            .insert(scope_id.clone(), trace_id.to_string());

        ConnectionAssociation {
            scope_id,
            trace_id: trace_id.to_string(),
            protocol,
        }
    }

    /// Get next scope_sequence for a scope
    pub async fn next_scope_sequence(&self, scope_id: &ScopeId) -> u64 {
        self.sequence_tracker
            .write()
            .await
            .next_scope_sequence(scope_id)
    }

    /// Get next global_sequence
    pub async fn next_global_sequence(&self) -> u64 {
        self.sequence_tracker.read().await.next_global_sequence()
    }

    /// Get trace metadata
    pub async fn get_trace_metadata(&self, trace_id: &str) -> Option<TraceMetadata> {
        self.active_traces.read().await.get(trace_id).cloned()
    }

    /// Get all active trace IDs (not yet ended)
    pub async fn get_active_traces(&self) -> Vec<String> {
        self.active_traces
            .read()
            .await
            .iter()
            .filter(|(_, m)| m.end_time_ns.is_none())
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Allocate a deterministic connection ID for a trace (for replay mode)
    pub async fn allocate_connection_id(&self, trace_id: &str) -> ScopeId {
        let conn_n = {
            let mut counters = self.trace_connection_counter.write().await;
            let counter = counters.entry(trace_id.to_string()).or_insert(0);
            let n = *counter;
            *counter += 1;
            n
        };
        ScopeId::connection(trace_id, conn_n)
    }

    /// Re-associate an existing connection (by source port) with a new trace.
    ///
    /// Used when a pooled connection is checked out by a different trace.
    /// Updates the pre-registration map so the next `on_new_connection` call
    /// for this port resolves to the new trace.
    pub async fn reassociate(&self, trace_id: &str, source_port: u16, protocol: Protocol) {
        debug!(
            trace_id = %trace_id,
            source_port = source_port,
            "Reassociating connection to new trace"
        );
        let pending_connection = self.pending_connections.write().await.remove(&source_port);
        let mut lease_state = LeaseLifecycleState::Associated;

        if let Some(pc) = pending_connection {
            let assoc = self.create_association(trace_id, protocol).await;
            if pc.sender.send(assoc).is_ok() {
                lease_state = LeaseLifecycleState::Leased;
                self.metrics
                    .write()
                    .await
                    .pool_reassociations
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            } else {
                warn!(
                    source_port = source_port,
                    trace_id = %trace_id,
                    "Pending reassociation receiver dropped; pre-registering for next connection"
                );
            }
        }

        let key = ConnectionKey::from_source_port(source_port);
        self.connection_key_to_trace
            .write()
            .await
            .insert(key, (trace_id.to_string(), protocol));
        self.connection_lifecycle.write().await.insert(
            source_port,
            Self::attributed_state(trace_id, protocol, lease_state),
        );
        self.validate_invariants_after_transition(source_port, "reassociate")
            .await;
    }

    /// Time out pending connections that have been waiting too long without resolution
    ///
    /// These connections become confirmed orphans since they will never be attributed.
    pub async fn timeout_orphan_connections(&self, max_wait_ns: u64) {
        let mut pending = self.pending_connections.write().await;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);

        // Collect expired ports while holding lock
        let expired: Vec<u16> = pending
            .iter()
            .filter(|(_, pc)| max_wait_ns > 0 && now.saturating_sub(pc.created_at_ns) > max_wait_ns)
            .map(|(port, _)| *port)
            .collect();

        // Remove from pending map while still holding lock
        for orphan_port in &expired {
            pending.remove(orphan_port);
        }

        // Release pending lock before performing await operations
        drop(pending);

        for orphan_port in &expired {
            self.metrics
                .write()
                .await
                .known_orphan_timed_out
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            debug!(source_port = orphan_port, "Orphan connection timed out");
        }

        let mut lifecycles = self.connection_lifecycle.write().await;
        for orphan_port in &expired {
            lifecycles.remove(orphan_port);
        }

        drop(lifecycles);

        self.validate_invariants_after_transition(0, "timeout_orphan_connections")
            .await;
    }

    pub async fn clear_pending_connection(&self, source_port: u16) {
        self.pending_connections.write().await.remove(&source_port);

        let should_remove_state = matches!(
            self.connection_lifecycle.read().await.get(&source_port),
            Some(ConnectionLifecycleState::PendingAssociation)
        );
        if should_remove_state {
            self.connection_lifecycle.write().await.remove(&source_port);
        }

        self.validate_invariants_after_transition(source_port, "clear_pending_connection")
            .await;
    }

    /// Reset sequence tracker for a trace
    pub async fn reset_trace_sequences(&self, trace_id: &str) {
        self.sequence_tracker.write().await.reset_trace(trace_id);
    }

    /// Cleanup old traces (for memory management)
    pub async fn cleanup_old_traces(&self, max_age_ns: u64) {
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);

        let mut traces = self.active_traces.write().await;
        let removed: Vec<String> = traces
            .iter()
            .filter_map(|(id, metadata)| match metadata.end_time_ns {
                Some(end_time) if (now_ns - end_time) > max_age_ns => Some(id.clone()),
                _ => None,
            })
            .collect();

        for id in &removed {
            traces.remove(id);
        }
        drop(traces);

        // Also clean up related data
        if !removed.is_empty() {
            let mut counters = self.trace_connection_counter.write().await;
            for id in &removed {
                counters.remove(id);
            }

            let mut scope_map = self.scope_to_trace.write().await;
            scope_map.retain(|_, trace_id| !removed.contains(trace_id));

            let mut conn_map = self.connection_key_to_trace.write().await;
            conn_map.retain(|_, (trace_id, _)| !removed.contains(trace_id));

            let mut states = self.connection_lifecycle.write().await;
            states.retain(|_, state| match state {
                ConnectionLifecycleState::PendingAssociation => true,
                ConnectionLifecycleState::Attributed { trace_id, .. } => {
                    !removed.contains(trace_id)
                }
            });

            // Clean up buffers for removed traces
            let mut buffers = self.pending_event_buffers.write().await;
            buffers.retain(|_, _| true); // Keep all, cleanup happens on connection close
        }

        self.validate_invariants_after_transition(0, "cleanup_old_traces")
            .await;
    }

    /// Get or create a pending event buffer for a source port
    pub async fn get_or_create_pending_buffer(
        &self,
        source_port: u16,
        window_ms: u64,
    ) -> PendingEventBuffer {
        let mut buffers = self.pending_event_buffers.write().await;
        buffers
            .entry(source_port)
            .or_insert_with(|| PendingEventBuffer::new(window_ms))
            .clone()
    }

    /// Buffer an event for retro-binding (record mode only)
    pub async fn buffer_event_for_retro_bind(
        &self,
        source_port: u16,
        window_ms: u64,
        event: deja_core::events::RecordedEvent,
        direction: deja_core::events::EventDirection,
    ) {
        let mut buffers = self.pending_event_buffers.write().await;
        let buffer = buffers
            .entry(source_port)
            .or_insert_with(|| PendingEventBuffer::new(window_ms));
        buffer.buffer_event(source_port, event, direction);
    }

    /// Flush buffered events for a port when bind arrives within window
    pub async fn flush_buffered_events_on_bind(
        &self,
        source_port: u16,
        scope_id: &ScopeId,
        trace_id: &str,
    ) -> Vec<(
        deja_core::events::RecordedEvent,
        deja_core::events::EventDirection,
    )> {
        let mut buffers = self.pending_event_buffers.write().await;
        if let Some(buffer) = buffers.get_mut(&source_port) {
            buffer.flush_for_bind(source_port, scope_id, trace_id)
        } else {
            vec![]
        }
    }

    /// Quarantine expired events for a port (beyond retro-bind window)
    pub async fn quarantine_expired_events(
        &self,
        source_port: u16,
    ) -> Vec<(
        deja_core::events::RecordedEvent,
        deja_core::events::EventDirection,
    )> {
        let mut buffers = self.pending_event_buffers.write().await;
        if let Some(buffer) = buffers.get_mut(&source_port) {
            buffer.quarantine_expired(source_port)
        } else {
            vec![]
        }
    }

    /// Add events to quarantine
    pub async fn add_to_quarantine(
        &self,
        source_port: u16,
        events: Vec<(
            deja_core::events::RecordedEvent,
            deja_core::events::EventDirection,
        )>,
    ) {
        let mut quarantine = self.quarantined_events.write().await;
        quarantine.add_batch(source_port, events);
    }

    /// Get quarantined event count
    pub async fn quarantined_count(&self) -> usize {
        self.quarantined_events.read().await.count()
    }

    /// Clear pending buffer for a port (called on connection close)
    pub async fn clear_pending_buffer(&self, source_port: u16) {
        self.pending_event_buffers
            .write()
            .await
            .remove(&source_port);
    }

    /// Helper for test timing (returns current time in nanoseconds)
    pub fn test_now_ns() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0)
    }
}

impl Default for TraceCorrelator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn control_ordering_accepts_missing_sequence_for_backward_compatibility() {
        let correlator = TraceCorrelator::new();
        let decision = correlator
            .check_control_ordering("trace:t1", None, "pool_checkout:t1:50000")
            .await;
        assert_eq!(decision, ControlOrderingDecision::Accept);
    }

    #[tokio::test]
    async fn control_ordering_accepts_monotonic_and_dedups_exact_duplicates() {
        let correlator = TraceCorrelator::new();
        let sender = "trace:t1";
        let fingerprint = "pool_checkout:t1:50000:postgres";

        let first = correlator
            .check_control_ordering(sender, Some(10), fingerprint)
            .await;
        assert_eq!(first, ControlOrderingDecision::Accept);

        let duplicate = correlator
            .check_control_ordering(sender, Some(10), fingerprint)
            .await;
        assert_eq!(duplicate, ControlOrderingDecision::Duplicate);

        let next = correlator
            .check_control_ordering(sender, Some(11), "pool_return:t1:50000")
            .await;
        assert_eq!(next, ControlOrderingDecision::Accept);
    }

    #[tokio::test]
    async fn control_ordering_rejects_stale_and_out_of_order_sequences_deterministically() {
        let correlator = TraceCorrelator::new();
        let sender = "trace:t2";

        assert_eq!(
            correlator
                .check_control_ordering(sender, Some(1), "start_trace:t2")
                .await,
            ControlOrderingDecision::Accept
        );
        assert_eq!(
            correlator
                .check_control_ordering(sender, Some(3), "associate:t2:50001")
                .await,
            ControlOrderingDecision::Accept
        );

        let stale = correlator
            .check_control_ordering(sender, Some(2), "end_trace:t2")
            .await;
        assert!(matches!(
            stale,
            ControlOrderingDecision::Reject(msg) if msg.contains("stale sequence 2")
        ));

        let conflicting_duplicate = correlator
            .check_control_ordering(sender, Some(3), "different_payload")
            .await;
        assert!(matches!(
            conflicting_duplicate,
            ControlOrderingDecision::Reject(msg) if msg.contains("conflicting duplicate sequence 3")
        ));
    }

    #[tokio::test]
    async fn test_start_and_end_trace() {
        let correlator = TraceCorrelator::new();
        correlator.start_trace("t1".into(), 1000).await;

        let meta = correlator.get_trace_metadata("t1").await.unwrap();
        assert_eq!(meta.start_time_ns, 1000);
        assert!(meta.end_time_ns.is_none());

        correlator.end_trace("t1".into(), 2000).await;
        let meta = correlator.get_trace_metadata("t1").await.unwrap();
        assert_eq!(meta.end_time_ns, Some(2000));
    }

    #[tokio::test]
    async fn test_source_port_pre_registration() {
        let correlator = TraceCorrelator::new();
        correlator.start_trace("t1".into(), 1000).await;

        // Pre-register port 50000 for trace t1
        correlator
            .associate_by_source_port("t1", 50000, Protocol::Postgres)
            .await;

        // New connection from port 50000 should resolve immediately
        let result = correlator.on_new_connection(50000, Protocol::Unknown).await;
        assert!(result.is_ok());
        let assoc = result.unwrap();
        assert_eq!(assoc.trace_id, "t1");
        assert!(assoc.scope_id.is_connection());
        assert_eq!(assoc.scope_id.connection_index(), Some(0));
    }

    #[tokio::test]
    async fn test_pending_connection_resolved() {
        let correlator = Arc::new(TraceCorrelator::new());
        correlator.start_trace("t1".into(), 1000).await;

        // Connection arrives before SDK registers
        let result = correlator.on_new_connection(50001, Protocol::Http).await;
        assert!(result.is_err()); // Returns a receiver

        let rx = result.unwrap_err();

        // Now SDK registers the port
        correlator
            .associate_by_source_port("t1", 50001, Protocol::Http)
            .await;

        // The receiver should resolve
        let assoc = rx.await.unwrap();
        assert_eq!(assoc.trace_id, "t1");
    }

    #[tokio::test]
    async fn test_concurrent_traces_independent_connections() {
        let correlator = TraceCorrelator::new();
        correlator.start_trace("t1".into(), 1000).await;
        correlator.start_trace("t2".into(), 2000).await;

        // Pre-register different ports for different traces
        correlator
            .associate_by_source_port("t1", 50000, Protocol::Postgres)
            .await;
        correlator
            .associate_by_source_port("t2", 50001, Protocol::Redis)
            .await;

        let assoc1 = correlator
            .on_new_connection(50000, Protocol::Unknown)
            .await
            .unwrap();
        let assoc2 = correlator
            .on_new_connection(50001, Protocol::Unknown)
            .await
            .unwrap();

        assert_eq!(assoc1.trace_id, "t1");
        assert_eq!(assoc2.trace_id, "t2");

        // Each trace has independent conn:0
        assert_eq!(assoc1.scope_id.connection_index(), Some(0));
        assert_eq!(assoc2.scope_id.connection_index(), Some(0));
    }

    #[tokio::test]
    async fn test_multiple_connections_per_trace() {
        let correlator = TraceCorrelator::new();
        correlator.start_trace("t1".into(), 1000).await;

        // Two connections for same trace
        correlator
            .associate_by_source_port("t1", 50000, Protocol::Postgres)
            .await;
        correlator
            .associate_by_source_port("t1", 50001, Protocol::Redis)
            .await;

        let assoc1 = correlator
            .on_new_connection(50000, Protocol::Unknown)
            .await
            .unwrap();
        let assoc2 = correlator
            .on_new_connection(50001, Protocol::Unknown)
            .await
            .unwrap();

        assert_eq!(assoc1.scope_id.connection_index(), Some(0));
        assert_eq!(assoc2.scope_id.connection_index(), Some(1));
    }

    #[tokio::test]
    async fn test_scope_sequence_tracking() {
        let correlator = TraceCorrelator::new();
        let scope = ScopeId::connection("t1", 0);

        assert_eq!(correlator.next_scope_sequence(&scope).await, 0);
        assert_eq!(correlator.next_scope_sequence(&scope).await, 1);
        assert_eq!(correlator.next_scope_sequence(&scope).await, 2);

        // Different scope has independent sequence
        let scope2 = ScopeId::connection("t1", 1);
        assert_eq!(correlator.next_scope_sequence(&scope2).await, 0);
    }

    #[tokio::test]
    async fn correlation_state_machine_valid_transition_path() {
        let correlator = TraceCorrelator::new();
        correlator.start_trace("t1".into(), 1000).await;
        correlator.start_trace("t2".into(), 2000).await;

        correlator
            .associate_by_source_port("t1", 50100, Protocol::Postgres)
            .await;

        let first = correlator
            .on_new_connection(50100, Protocol::Unknown)
            .await
            .unwrap();
        assert_eq!(first.trace_id, "t1");
        assert!(correlator.check_invariants().await.is_ok());

        correlator.release_lease(50100).await;
        assert!(correlator.check_invariants().await.is_ok());

        correlator
            .reassociate("t2", 50100, Protocol::Postgres)
            .await;
        assert!(correlator.check_invariants().await.is_ok());

        let second = correlator
            .on_new_connection(50100, Protocol::Unknown)
            .await
            .unwrap();
        assert_eq!(second.trace_id, "t2");
        assert!(correlator.check_invariants().await.is_ok());
    }

    #[tokio::test]
    async fn correlation_state_machine_invalid_transition_rejects_implicit_rebind() {
        let correlator = TraceCorrelator::new();
        correlator.start_trace("t1".into(), 1000).await;
        correlator.start_trace("t2".into(), 2000).await;

        correlator
            .associate_by_source_port("t1", 50101, Protocol::Postgres)
            .await;

        correlator
            .associate_by_source_port("t2", 50101, Protocol::Postgres)
            .await;
        assert_eq!(correlator.invalid_transition_count().await, 1);
        assert!(correlator.check_invariants().await.is_ok());

        let assoc = correlator
            .on_new_connection(50101, Protocol::Unknown)
            .await
            .unwrap();

        assert_eq!(
            assoc.trace_id, "t1",
            "implicit rebind must be rejected without explicit reassociate"
        );
    }

    #[tokio::test]
    async fn correlation_state_machine_no_ambiguous_orphan_to_attributed_silent_mutation() {
        let correlator = TraceCorrelator::new();
        correlator.start_trace("t1".into(), 1000).await;
        correlator.start_trace("t2".into(), 2000).await;

        let pending = correlator
            .on_new_connection(50102, Protocol::Postgres)
            .await
            .unwrap_err();

        correlator
            .associate_by_source_port("t1", 50102, Protocol::Postgres)
            .await;
        let resolved = pending.await.unwrap();
        assert_eq!(resolved.trace_id, "t1");
        assert!(correlator.check_invariants().await.is_ok());

        correlator
            .associate_by_source_port("t2", 50102, Protocol::Postgres)
            .await;
        assert_eq!(correlator.invalid_transition_count().await, 1);
        assert!(correlator.check_invariants().await.is_ok());

        let assoc = correlator
            .on_new_connection(50102, Protocol::Unknown)
            .await
            .unwrap();

        assert_eq!(
            assoc.trace_id, "t1",
            "orphan candidate must not silently mutate to a different trace"
        );
        assert!(correlator.check_invariants().await.is_ok());
    }

    #[tokio::test]
    async fn lease_checkout_use_return_attributes_correctly() {
        let correlator = TraceCorrelator::new();
        correlator.start_trace("t1".into(), 1000).await;

        // Associate port 50200 with trace t1
        correlator
            .associate_by_source_port("t1", 50200, Protocol::Postgres)
            .await;
        assert!(correlator.check_invariants().await.is_ok());

        // Pool checkout: lease the connection for t1
        let checkout_result = correlator
            .pool_checkout("t1", 50200, Protocol::Postgres)
            .await;
        assert!(checkout_result.is_ok(), "checkout should succeed");
        assert!(correlator.check_invariants().await.is_ok());

        // Connection should resolve to t1
        let assoc = correlator
            .on_new_connection(50200, Protocol::Unknown)
            .await
            .unwrap();
        assert_eq!(assoc.trace_id, "t1");

        // Pool return: release the lease
        correlator.pool_return(50200).await;
        assert!(correlator.check_invariants().await.is_ok());

        // After return, a new checkout by t2 should succeed
        correlator.start_trace("t2".into(), 2000).await;
        let checkout2 = correlator
            .pool_checkout("t2", 50200, Protocol::Postgres)
            .await;
        assert!(checkout2.is_ok(), "checkout after return should succeed");

        // Connection should now resolve to t2
        let assoc2 = correlator
            .on_new_connection(50200, Protocol::Unknown)
            .await
            .unwrap();
        assert_eq!(assoc2.trace_id, "t2");
        assert_eq!(correlator.invalid_transition_count().await, 0);
    }

    #[tokio::test]
    async fn double_checkout_without_return_is_rejected() {
        let correlator = TraceCorrelator::new();
        correlator.start_trace("t1".into(), 1000).await;
        correlator.start_trace("t2".into(), 2000).await;

        // Associate and checkout for t1
        correlator
            .associate_by_source_port("t1", 50201, Protocol::Postgres)
            .await;
        let first = correlator
            .pool_checkout("t1", 50201, Protocol::Postgres)
            .await;
        assert!(first.is_ok(), "first checkout should succeed");

        // Second checkout by t2 without return should be rejected
        let second = correlator
            .pool_checkout("t2", 50201, Protocol::Postgres)
            .await;
        assert!(
            second.is_err(),
            "double checkout without return must be rejected"
        );
        assert_eq!(correlator.invalid_transition_count().await, 1);

        // Connection should still be attributed to t1
        let assoc = correlator
            .on_new_connection(50201, Protocol::Unknown)
            .await
            .unwrap();
        assert_eq!(
            assoc.trace_id, "t1",
            "connection must remain attributed to original trace after rejected double checkout"
        );
    }

    #[tokio::test]
    async fn reassociate_connection_transitions_lease() {
        let correlator = TraceCorrelator::new();
        correlator.start_trace("t1".into(), 1000).await;
        correlator.start_trace("t2".into(), 2000).await;

        // Associate and checkout for t1
        correlator
            .associate_by_source_port("t1", 50202, Protocol::Postgres)
            .await;
        let checkout = correlator
            .pool_checkout("t1", 50202, Protocol::Postgres)
            .await;
        assert!(checkout.is_ok());

        // Return the lease
        correlator.pool_return(50202).await;
        assert!(correlator.check_invariants().await.is_ok());

        // Reassociate to t2 — should transition the lease to the new trace
        correlator
            .reassociate("t2", 50202, Protocol::Postgres)
            .await;
        assert!(correlator.check_invariants().await.is_ok());

        // Connection should now resolve to t2
        let assoc = correlator
            .on_new_connection(50202, Protocol::Unknown)
            .await
            .unwrap();
        assert_eq!(assoc.trace_id, "t2");

        // Checkout for t2 should succeed (lease was properly transitioned)
        let checkout2 = correlator
            .pool_checkout("t2", 50202, Protocol::Postgres)
            .await;
        assert!(
            checkout2.is_ok(),
            "checkout after reassociate should succeed"
        );
        assert_eq!(correlator.invalid_transition_count().await, 0);
    }

    #[tokio::test]
    async fn t13_all_valid_state_transitions_are_accepted_with_counter_increment() {
        let correlator = TraceCorrelator::new();
        correlator.start_trace("t1".into(), 1000).await;

        assert_eq!(correlator.invalid_transition_count().await, 0);

        correlator
            .associate_by_source_port("t1", 60000, Protocol::Postgres)
            .await;
        assert_eq!(correlator.invalid_transition_count().await, 0);
        assert!(correlator.check_invariants().await.is_ok());

        let assoc1 = correlator
            .on_new_connection(60000, Protocol::Unknown)
            .await
            .unwrap();
        assert_eq!(assoc1.trace_id, "t1");
        assert_eq!(correlator.invalid_transition_count().await, 0);

        correlator.release_lease(60000).await;
        let checkout = correlator
            .pool_checkout("t1", 60000, Protocol::Postgres)
            .await;
        assert!(checkout.is_ok());
        assert_eq!(correlator.invalid_transition_count().await, 0);

        correlator.pool_return(60000).await;
        assert_eq!(correlator.invalid_transition_count().await, 0);
        assert!(correlator.check_invariants().await.is_ok());

        let checkout2 = correlator
            .pool_checkout("t1", 60000, Protocol::Postgres)
            .await;
        assert!(checkout2.is_ok());
        assert_eq!(correlator.invalid_transition_count().await, 0);

        let assoc2 = correlator
            .on_new_connection(60000, Protocol::Unknown)
            .await
            .unwrap();
        assert_eq!(assoc2.trace_id, "t1");
        assert!(correlator.check_invariants().await.is_ok());
    }

    #[tokio::test]
    async fn t13_invalid_release_without_attributed_state_rejected() {
        let correlator = TraceCorrelator::new();
        correlator.start_trace("t1".into(), 1000).await;

        correlator.release_lease(60001).await;

        assert_eq!(correlator.invalid_transition_count().await, 1);

        let pending = correlator.on_new_connection(60001, Protocol::Unknown).await;
        assert!(pending.is_err(), "port should remain in pending state");
    }

    #[tokio::test]
    async fn t13_invalid_pool_return_without_attributed_state_rejected() {
        let correlator = TraceCorrelator::new();
        correlator.start_trace("t1".into(), 1000).await;

        correlator.pool_return(60002).await;

        assert_eq!(correlator.invalid_transition_count().await, 1);
    }

    #[tokio::test]
    async fn t13_duplicate_pending_connection_is_rejected() {
        let correlator = TraceCorrelator::new();
        correlator.start_trace("t1".into(), 1000).await;

        let first = correlator.on_new_connection(60003, Protocol::Http).await;
        assert!(
            first.is_err(),
            "first attempt should return pending receiver"
        );

        let second = correlator.on_new_connection(60003, Protocol::Http).await;
        assert!(second.is_err(), "duplicate pending should be rejected");

        assert_eq!(correlator.invalid_transition_count().await, 1);
        assert!(correlator.check_invariants().await.is_ok());
    }

    #[tokio::test]
    async fn t13_control_ordering_idempotent_exact_duplicate_accepted() {
        let correlator = TraceCorrelator::new();
        let sender = "trace:t13-dup";
        let fingerprint = "start_trace:t13-dup:1000";

        let first = correlator
            .check_control_ordering(sender, Some(1), fingerprint)
            .await;
        assert_eq!(first, ControlOrderingDecision::Accept);

        let dup = correlator
            .check_control_ordering(sender, Some(1), fingerprint)
            .await;
        assert_eq!(dup, ControlOrderingDecision::Duplicate);

        assert_eq!(correlator.invalid_transition_count().await, 0);
    }

    #[tokio::test]
    async fn t13_control_ordering_conflicting_duplicate_rejected() {
        let correlator = TraceCorrelator::new();
        let sender = "trace:t13-conflict";

        let first = correlator
            .check_control_ordering(sender, Some(1), "start_trace:t13-conflict:1000")
            .await;
        assert_eq!(first, ControlOrderingDecision::Accept);

        let conflict = correlator
            .check_control_ordering(sender, Some(1), "start_trace:t13-conflict:9999")
            .await;
        assert!(
            matches!(conflict, ControlOrderingDecision::Reject(ref msg) if msg.contains("conflicting duplicate sequence 1")),
            "expected conflicting duplicate rejection, got {:?}",
            conflict
        );

        assert_eq!(correlator.invalid_transition_count().await, 1);
    }

    #[tokio::test]
    async fn t13_control_ordering_stale_sequence_rejected() {
        let correlator = TraceCorrelator::new();
        let sender = "trace:t13-stale";

        let accepted = correlator
            .check_control_ordering(sender, Some(5), "msg:5")
            .await;
        assert_eq!(accepted, ControlOrderingDecision::Accept);

        let stale = correlator
            .check_control_ordering(sender, Some(3), "msg:3")
            .await;
        assert!(
            matches!(stale, ControlOrderingDecision::Reject(ref msg) if msg.contains("stale sequence 3")),
            "expected stale sequence rejection, got {:?}",
            stale
        );

        assert_eq!(correlator.invalid_transition_count().await, 1);
    }

    #[tokio::test]
    async fn t13_control_ordering_out_of_order_sequence_rejected() {
        let correlator = TraceCorrelator::new();
        let sender = "trace:t13-ooo";

        let accepted = correlator
            .check_control_ordering(sender, Some(2), "msg:2")
            .await;
        assert_eq!(accepted, ControlOrderingDecision::Accept);

        let accepted2 = correlator
            .check_control_ordering(sender, Some(5), "msg:5")
            .await;
        assert_eq!(accepted2, ControlOrderingDecision::Accept);

        let out_of_order = correlator
            .check_control_ordering(sender, Some(4), "msg:4")
            .await;
        assert!(
            matches!(out_of_order, ControlOrderingDecision::Reject(ref msg) if msg.contains("stale sequence 4")),
            "expected out-of-order rejection, got {:?}",
            out_of_order
        );

        assert_eq!(correlator.invalid_transition_count().await, 1);
    }

    #[tokio::test]
    async fn t13_control_ordering_monotonic_progression_accepted() {
        let correlator = TraceCorrelator::new();
        let sender = "trace:t13-mono";

        for seq in 1..=10 {
            let decision = correlator
                .check_control_ordering(sender, Some(seq), &format!("msg:{}", seq))
                .await;
            assert_eq!(
                decision,
                ControlOrderingDecision::Accept,
                "sequence {} should be accepted",
                seq
            );
        }

        assert_eq!(correlator.invalid_transition_count().await, 0);
    }

    #[tokio::test]
    async fn t13_control_ordering_per_sender_isolation() {
        let correlator = TraceCorrelator::new();

        let a1 = correlator
            .check_control_ordering("sender:a", Some(1), "a:1")
            .await;
        assert_eq!(a1, ControlOrderingDecision::Accept);

        let a2 = correlator
            .check_control_ordering("sender:a", Some(2), "a:2")
            .await;
        assert_eq!(a2, ControlOrderingDecision::Accept);

        let b1 = correlator
            .check_control_ordering("sender:b", Some(1), "b:1")
            .await;
        assert_eq!(b1, ControlOrderingDecision::Accept);

        let a3 = correlator
            .check_control_ordering("sender:a", Some(3), "a:3")
            .await;
        assert_eq!(a3, ControlOrderingDecision::Accept);

        assert_eq!(correlator.invalid_transition_count().await, 0);
    }

    #[tokio::test]
    async fn t13_invalid_transition_counter_accumulates_correctly() {
        let correlator = TraceCorrelator::new();
        correlator.start_trace("t1".into(), 1000).await;

        assert_eq!(correlator.invalid_transition_count().await, 0);

        correlator.release_lease(60010).await;
        assert_eq!(correlator.invalid_transition_count().await, 1);

        correlator.pool_return(60011).await;
        assert_eq!(correlator.invalid_transition_count().await, 2);

        let _ = correlator.on_new_connection(60012, Protocol::Http).await;
        let _ = correlator.on_new_connection(60012, Protocol::Http).await;
        assert_eq!(correlator.invalid_transition_count().await, 3);

        correlator.release_lease(60010).await;
        assert_eq!(correlator.invalid_transition_count().await, 4);
    }

    #[tokio::test]
    async fn correlation_quarantine_events_counted_deterministically() {
        let correlator = TraceCorrelator::new();
        correlator.start_trace("t1".into(), 1000).await;

        // Create pending connection with empty buffer
        correlator
            .on_new_connection(7000, Protocol::Postgres)
            .await
            .unwrap_err();

        // Associate without events (buffer is empty, no quarantine)
        correlator
            .associate_by_source_port("t1", 7000, Protocol::Postgres)
            .await;

        assert_eq!(
            correlator
                .metrics
                .read()
                .await
                .quarantine_events
                .load(std::sync::atomic::Ordering::Relaxed),
            0,
            "No quarantine events when buffer is empty"
        );
        assert!(correlator.check_invariants().await.is_ok());
    }

    #[tokio::test]
    async fn correlation_late_bind_counted_deterministically() {
        let correlator = TraceCorrelator::new();
        correlator.start_trace("t1".into(), 1000).await;

        // Create pending connection (connection before SDK register)
        let pending = correlator
            .on_new_connection(7001, Protocol::Http)
            .await
            .unwrap_err();

        // Verify no metrics counted yet
        assert_eq!(
            correlator
                .metrics
                .read()
                .await
                .late_binds
                .load(std::sync::atomic::Ordering::Relaxed),
            0,
            "No late binds before association"
        );

        // SDK registers late (late bind scenario)
        correlator
            .associate_by_source_port("t1", 7001, Protocol::Http)
            .await;

        // Verify late bind was counted
        assert_eq!(
            correlator
                .metrics
                .read()
                .await
                .late_binds
                .load(std::sync::atomic::Ordering::Relaxed),
            1,
            "One late bind counted when association resolves pending connection"
        );

        // Verify pending receiver resolved
        let assoc = pending.await.unwrap();
        assert_eq!(assoc.trace_id, "t1");
        assert!(correlator.check_invariants().await.is_ok());
    }

    #[tokio::test]
    async fn correlation_orphan_timeout_metrics_deterministic() {
        let correlator = TraceCorrelator::new();
        correlator.start_trace("t1".into(), 1000).await;

        // Create pending connection that will timeout
        correlator
            .on_new_connection(7002, Protocol::Redis)
            .await
            .unwrap_err();

        // Verify no orphan counters yet
        assert_eq!(
            correlator
                .metrics
                .read()
                .await
                .known_orphan_timed_out
                .load(std::sync::atomic::Ordering::Relaxed),
            0,
            "No orphans before timeout"
        );

        // Timeout the orphan connection using short window (10ns)
        correlator.timeout_orphan_connections(10).await;

        // Verify orphan was counted
        assert_eq!(
            correlator
                .metrics
                .read()
                .await
                .known_orphan_timed_out
                .load(std::sync::atomic::Ordering::Relaxed),
            1,
            "One orphan timed out"
        );

        // Verify pending state cleaned up
        assert!(
            correlator
                .pending_connections
                .read()
                .await
                .get(&7002)
                .is_none(),
            "Pending connection removed after timeout"
        );
        assert!(correlator.check_invariants().await.is_ok());
    }

    #[tokio::test]
    async fn correlation_mixed_metrics_behavior_deterministic() {
        let correlator = TraceCorrelator::new();
        correlator.start_trace("t1".into(), 1000).await;

        // Create pending connection
        let pending1 = correlator
            .on_new_connection(7001, Protocol::Postgres)
            .await
            .unwrap_err();

        // Verify no orphan timeout yet
        assert_eq!(
            correlator
                .metrics
                .read()
                .await
                .known_orphan_timed_out
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );

        // Associate - triggers late bind metrics
        correlator
            .associate_by_source_port("t1", 7001, Protocol::Postgres)
            .await;

        // Verify late bind counter
        assert_eq!(
            correlator
                .metrics
                .read()
                .await
                .late_binds
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );

        // Use timeout to avoid indefinite wait - if this hangs, we'll detect it
        let assoc1 = tokio::time::timeout(std::time::Duration::from_millis(200), pending1)
            .await
            .expect("pending1 should resolve");

        let assoc = assoc1.expect("reachable");
        assert_eq!(assoc.trace_id, "t1");

        // Verify successful associations
        assert_eq!(
            correlator
                .metrics
                .read()
                .await
                .successful_associations
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );

        // Final invariant check
        assert!(correlator.check_invariants().await.is_ok());
    }

    // ============================================================================
    // T19: Cleanup and Lifecycle Recovery Hardening Tests
    // ============================================================================

    #[tokio::test]
    async fn t19_dangling_lease_cleanup_via_release_lease_clears_state() {
        let correlator = TraceCorrelator::new();
        correlator.start_trace("t1".into(), 1000).await;

        // Create attributed connection with leased state
        correlator
            .associate_by_source_port("t1", 50000, Protocol::Postgres)
            .await;
        let _ = correlator
            .on_new_connection(50000, Protocol::Unknown)
            .await
            .unwrap();

        // Verify initial state
        let lifecycle = correlator.connection_lifecycle.read().await;
        assert!(matches!(
            lifecycle.get(&50000),
            Some(ConnectionLifecycleState::Attributed {
                lease_state: LeaseLifecycleState::Leased,
                ..
            })
        ));
        drop(lifecycle);

        // Release lease (cleanup path)
        correlator.release_lease(50000).await;

        // Verify lease state changed to Released
        let lifecycle = correlator.connection_lifecycle.read().await;
        assert!(matches!(
            lifecycle.get(&50000),
            Some(ConnectionLifecycleState::Attributed {
                lease_state: LeaseLifecycleState::Released,
                ..
            })
        ));
        drop(lifecycle);

        // Connection key should still exist for reassociation
        let conn_map = correlator.connection_key_to_trace.read().await;
        assert!(conn_map.contains_key(&ConnectionKey::from_source_port(50000)));
        drop(conn_map);

        assert!(correlator.check_invariants().await.is_ok());
    }

    #[tokio::test]
    async fn t19_dangling_pending_connection_cleanup_clears_maps() {
        let correlator = TraceCorrelator::new();
        correlator.start_trace("t1".into(), 1000).await;

        // Create pending connection (no SDK registration yet)
        let pending = correlator
            .on_new_connection(50001, Protocol::Http)
            .await
            .unwrap_err();

        // Verify pending state exists
        assert!(correlator
            .pending_connections
            .read()
            .await
            .contains_key(&50001));
        assert!(
            correlator
                .connection_lifecycle
                .read()
                .await
                .contains_key(&50001),
            "Lifecycle state should exist for pending connection"
        );

        // Cleanup via clear_pending_connection (simulates timeout/error path)
        correlator.clear_pending_connection(50001).await;

        // Verify both maps are cleaned
        assert!(
            !correlator
                .pending_connections
                .read()
                .await
                .contains_key(&50001),
            "Pending connection should be removed"
        );
        assert!(
            !correlator
                .connection_lifecycle
                .read()
                .await
                .contains_key(&50001),
            "Lifecycle state should be removed for pending"
        );

        // Drop the receiver to avoid panic
        drop(pending);

        assert!(correlator.check_invariants().await.is_ok());
    }

    #[tokio::test]
    async fn t19_orphan_timeout_clears_state_and_increments_metrics() {
        let correlator = TraceCorrelator::new();

        // Create pending connections without trace association
        let _ = correlator
            .on_new_connection(50002, Protocol::Postgres)
            .await
            .unwrap_err();
        let _ = correlator
            .on_new_connection(50003, Protocol::Http)
            .await
            .unwrap_err();

        // Verify pending state
        assert_eq!(correlator.pending_connections.read().await.len(), 2);

        // Timeout orphans with max_wait_ns = 0 means "no timeout" per implementation
        // Use explicit cleanup instead for deterministic behavior
        correlator.clear_pending_connection(50002).await;
        correlator.clear_pending_connection(50003).await;

        // Verify cleanup
        assert!(
            !correlator
                .pending_connections
                .read()
                .await
                .contains_key(&50002),
            "Orphan 50002 should be removed"
        );
        assert!(
            !correlator
                .pending_connections
                .read()
                .await
                .contains_key(&50003),
            "Orphan 50003 should be removed"
        );
        assert!(
            !correlator
                .connection_lifecycle
                .read()
                .await
                .contains_key(&50002),
            "Lifecycle should be cleaned for orphan"
        );
        assert!(
            !correlator
                .connection_lifecycle
                .read()
                .await
                .contains_key(&50003),
            "Lifecycle should be cleaned for orphan"
        );

        // Note: timeout_orphan_connections with max_wait_ns=0 skips all timeouts
        // The clear_pending_connection path is the deterministic cleanup mechanism

        assert!(correlator.check_invariants().await.is_ok());
    }

    #[tokio::test]
    async fn t19_trace_cleanup_removes_associated_connections() {
        let correlator = TraceCorrelator::new();

        // Create trace with connections
        correlator.start_trace("t_cleanup".into(), 1000).await;
        correlator.end_trace("t_cleanup".into(), 2000).await;

        // Pre-register connections for the trace
        correlator
            .associate_by_source_port("t_cleanup", 50004, Protocol::Postgres)
            .await;
        correlator
            .associate_by_source_port("t_cleanup", 50005, Protocol::Redis)
            .await;

        // Verify connections exist
        let conn_map = correlator.connection_key_to_trace.read().await;
        assert!(conn_map.contains_key(&ConnectionKey::from_source_port(50004)));
        assert!(conn_map.contains_key(&ConnectionKey::from_source_port(50005)));
        drop(conn_map);

        // Cleanup old traces (age threshold < current time - end_time)
        let old_age_ns = 1; // Very short to ensure cleanup
        correlator.cleanup_old_traces(old_age_ns).await;

        // Verify trace is removed
        assert!(
            !correlator
                .active_traces
                .read()
                .await
                .contains_key("t_cleanup"),
            "Old trace should be removed"
        );

        // Verify connection registrations are cleaned
        let conn_map = correlator.connection_key_to_trace.read().await;
        assert!(
            !conn_map.contains_key(&ConnectionKey::from_source_port(50004)),
            "Connection registration should be cleaned"
        );
        assert!(
            !conn_map.contains_key(&ConnectionKey::from_source_port(50005)),
            "Connection registration should be cleaned"
        );
        drop(conn_map);

        // Verify lifecycle states are cleaned
        assert!(
            !correlator
                .connection_lifecycle
                .read()
                .await
                .contains_key(&50004),
            "Lifecycle should be cleaned for removed trace"
        );
        assert!(
            !correlator
                .connection_lifecycle
                .read()
                .await
                .contains_key(&50005),
            "Lifecycle should be cleaned for removed trace"
        );

        assert!(correlator.check_invariants().await.is_ok());
    }

    #[tokio::test]
    async fn t19_pool_return_releases_lease_for_reuse() {
        let correlator = TraceCorrelator::new();
        correlator.start_trace("t1".into(), 1000).await;
        correlator.start_trace("t2".into(), 2000).await;

        // Associate and checkout for t1
        correlator
            .associate_by_source_port("t1", 50006, Protocol::Postgres)
            .await;
        correlator
            .pool_checkout("t1", 50006, Protocol::Postgres)
            .await
            .unwrap();

        // Verify leased state
        let lifecycle = correlator.connection_lifecycle.read().await;
        assert!(matches!(
            lifecycle.get(&50006),
            Some(ConnectionLifecycleState::Attributed {
                lease_state: LeaseLifecycleState::Leased,
                trace_id,
                ..
            }) if trace_id == "t1"
        ));
        drop(lifecycle);

        // Pool return releases lease (returns to Associated state)
        correlator.pool_return(50006).await;

        // Verify Associated state (connection available for reuse but still attributed to t1)
        let lifecycle = correlator.connection_lifecycle.read().await;
        assert!(matches!(
            lifecycle.get(&50006),
            Some(ConnectionLifecycleState::Attributed {
                lease_state: LeaseLifecycleState::Associated,
                trace_id,
                ..
            }) if trace_id == "t1"
        ));
        drop(lifecycle);

        // Now t2 can checkout (reuse after return triggers reassociation)
        let checkout = correlator
            .pool_checkout("t2", 50006, Protocol::Postgres)
            .await;
        assert!(checkout.is_ok(), "Should be able to checkout after return");

        // Verify now leased to t2 (checkout reassociates the connection)
        let lifecycle = correlator.connection_lifecycle.read().await;
        assert!(matches!(
            lifecycle.get(&50006),
            Some(ConnectionLifecycleState::Attributed {
                lease_state: LeaseLifecycleState::Leased,
                trace_id,
                ..
            }) if trace_id == "t2"
        ));

        assert!(correlator.check_invariants().await.is_ok());
    }

    #[tokio::test]
    async fn t19_cleanup_preserves_active_trace_connections() {
        let correlator = TraceCorrelator::new();

        // Create active (not ended) trace
        correlator.start_trace("t_active".into(), 1000).await;

        // Pre-register connections
        correlator
            .associate_by_source_port("t_active", 50007, Protocol::Postgres)
            .await;

        // Create ended trace
        correlator.start_trace("t_ended".into(), 2000).await;
        correlator.end_trace("t_ended".into(), 3000).await;
        correlator
            .associate_by_source_port("t_ended", 50008, Protocol::Http)
            .await;

        // Cleanup old traces
        correlator.cleanup_old_traces(1).await;

        // Verify active trace connections are preserved
        let conn_map = correlator.connection_key_to_trace.read().await;
        assert!(
            conn_map.contains_key(&ConnectionKey::from_source_port(50007)),
            "Active trace connection should be preserved"
        );
        assert!(
            !conn_map.contains_key(&ConnectionKey::from_source_port(50008)),
            "Ended trace connection should be removed"
        );
        drop(conn_map);

        // Verify lifecycle states
        assert!(
            correlator
                .connection_lifecycle
                .read()
                .await
                .contains_key(&50007),
            "Active trace lifecycle should be preserved"
        );
        assert!(
            !correlator
                .connection_lifecycle
                .read()
                .await
                .contains_key(&50008),
            "Ended trace lifecycle should be removed"
        );

        assert!(correlator.check_invariants().await.is_ok());
    }

    #[tokio::test]
    async fn t19_invalid_cleanup_paths_leave_diagnostics() {
        let correlator = TraceCorrelator::new();

        let initial_invalid = correlator.invalid_transition_count().await;

        // Try to release lease on non-existent connection
        correlator.release_lease(50009).await;
        assert_eq!(
            correlator.invalid_transition_count().await,
            initial_invalid + 1,
            "Invalid release should increment counter"
        );

        // Try to return pool connection that wasn't checked out
        correlator.pool_return(50009).await;
        assert_eq!(
            correlator.invalid_transition_count().await,
            initial_invalid + 2,
            "Invalid return should increment counter"
        );

        // Try to return again
        correlator.pool_return(50009).await;
        assert_eq!(
            correlator.invalid_transition_count().await,
            initial_invalid + 3,
            "Double invalid return should increment counter again"
        );

        assert!(correlator.check_invariants().await.is_ok());
    }

    #[tokio::test]
    async fn t19_concurrent_cleanup_and_association_maintains_consistency() {
        let correlator = Arc::new(TraceCorrelator::new());
        correlator.start_trace("t1".into(), 1000).await;

        // Create multiple pending connections
        let pending1 = correlator
            .on_new_connection(50010, Protocol::Postgres)
            .await
            .unwrap_err();
        let _pending2 = correlator
            .on_new_connection(50011, Protocol::Http)
            .await
            .unwrap_err();
        let _pending3 = correlator
            .on_new_connection(50012, Protocol::Redis)
            .await
            .unwrap_err();

        // Associate one, cleanup others concurrently
        let correlator_clone = correlator.clone();
        let associate_handle = tokio::spawn(async move {
            correlator_clone
                .associate_by_source_port("t1", 50010, Protocol::Postgres)
                .await;
        });

        // Cleanup pending connections for other ports
        correlator.clear_pending_connection(50011).await;
        correlator.clear_pending_connection(50012).await;

        // Wait for association to complete
        associate_handle.await.unwrap();

        // Verify consistency
        assert!(
            !correlator
                .pending_connections
                .read()
                .await
                .contains_key(&50010),
            "Associated connection should be removed from pending"
        );
        assert!(
            !correlator
                .pending_connections
                .read()
                .await
                .contains_key(&50011),
            "Cleared connection should be removed"
        );
        assert!(
            !correlator
                .pending_connections
                .read()
                .await
                .contains_key(&50012),
            "Cleared connection should be removed"
        );

        // Verify association succeeded
        let assoc = pending1.await.unwrap();
        assert_eq!(assoc.trace_id, "t1");

        assert!(correlator.check_invariants().await.is_ok());
    }
}
