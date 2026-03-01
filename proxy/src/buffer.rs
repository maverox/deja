use deja_common::ScopeId;
use deja_core::events::{EventDirection, RecordedEvent};
use std::collections::HashMap;

#[derive(Clone, Debug)]
struct BufferedEvent {
    event: RecordedEvent,
    direction: EventDirection,
    buffered_at_ns: u64,
}

#[derive(Clone, Default)]
pub struct PendingEventBuffer {
    events: HashMap<u16, Vec<BufferedEvent>>,
    window_ms: u64,
}

impl PendingEventBuffer {
    pub fn new(window_ms: u64) -> Self {
        Self {
            events: HashMap::new(),
            window_ms,
        }
    }

    pub fn buffer_event(
        &mut self,
        peer_port: u16,
        event: RecordedEvent,
        direction: EventDirection,
    ) {
        self.events
            .entry(peer_port)
            .or_default()
            .push(BufferedEvent {
                event,
                direction,
                buffered_at_ns: current_time_ns(),
            });
    }

    pub fn flush_for_bind(
        &mut self,
        peer_port: u16,
        scope_id: &ScopeId,
        trace_id: &str,
    ) -> Vec<(RecordedEvent, EventDirection)> {
        let now = current_time_ns();
        let window_ns = self.window_ms * 1_000_000;

        let Some(buffered) = self.events.remove(&peer_port) else {
            return vec![];
        };

        let mut attributed = Vec::new();
        let mut expired = Vec::new();

        for entry in buffered {
            if now.saturating_sub(entry.buffered_at_ns) <= window_ns {
                let mut event = entry.event;
                event.trace_id = trace_id.to_string();
                event.scope_id = scope_id.as_str().to_string();
                attributed.push((event, entry.direction));
            } else {
                expired.push(entry);
            }
        }

        if !expired.is_empty() {
            self.events.insert(peer_port, expired);
        }

        attributed
    }

    pub fn quarantine_expired(&mut self, peer_port: u16) -> Vec<(RecordedEvent, EventDirection)> {
        self.events
            .remove(&peer_port)
            .unwrap_or_default()
            .into_iter()
            .map(|e| (e.event, e.direction))
            .collect()
    }

    pub fn has_events(&self, peer_port: u16) -> bool {
        self.events
            .get(&peer_port)
            .map(|v| !v.is_empty())
            .unwrap_or(false)
    }

    pub fn event_count(&self, peer_port: u16) -> usize {
        self.events.get(&peer_port).map(|v| v.len()).unwrap_or(0)
    }
}

#[derive(Clone, Default)]
pub struct QuarantinedEvents {
    events: Vec<(u16, RecordedEvent, EventDirection)>,
}

impl QuarantinedEvents {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, peer_port: u16, event: RecordedEvent, direction: EventDirection) {
        self.events.push((peer_port, event, direction));
    }

    pub fn add_batch(&mut self, peer_port: u16, events: Vec<(RecordedEvent, EventDirection)>) {
        for (event, direction) in events {
            self.events.push((peer_port, event, direction));
        }
    }

    pub fn count(&self) -> usize {
        self.events.len()
    }

    pub fn count_for_port(&self, peer_port: u16) -> usize {
        self.events
            .iter()
            .filter(|(p, _, _)| *p == peer_port)
            .count()
    }

    pub fn drain_all(&mut self) -> Vec<(u16, RecordedEvent, EventDirection)> {
        std::mem::take(&mut self.events)
    }
}

pub fn current_time_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}
