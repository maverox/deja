use crate::events::{recorded_event, RecordedEvent};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub struct EventHasher;

impl EventHasher {
    pub fn calculate_hash(event: &RecordedEvent) -> u64 {
        let mut hasher = DefaultHasher::new();

        // We only hash the "Request" part of the event.
        // And strictly specific fields to be stable across runs.

        match &event.event {
            Some(recorded_event::Event::HttpRequest(req)) => {
                // Http: Method, Path, Body
                // Ignore headers for now as they are often dynamic (Date, User-Agent versions, etc)
                // In future phase, we can have an allow-list of headers.
                "http".hash(&mut hasher);
                req.method.hash(&mut hasher);
                req.path.hash(&mut hasher);
                req.body.hash(&mut hasher);
            }
            Some(recorded_event::Event::RedisCommand(cmd)) => {
                // Redis: Command, Args
                "redis".hash(&mut hasher);
                cmd.command.to_uppercase().hash(&mut hasher); // Case insensitive command? Redis is case insensitive.
                for arg in &cmd.args {
                    // Match the RedisValue kind
                    // We need to implement hash for RedisValue manually or just inspect it
                    // Since it's generated protobuf, we access fields
                    if let Some(kind) = &arg.kind {
                        match kind {
                            crate::events::redis_value::Kind::Integer(i) => i.hash(&mut hasher),
                            crate::events::redis_value::Kind::BulkString(b) => b.hash(&mut hasher),
                            crate::events::redis_value::Kind::SimpleString(s) => {
                                s.hash(&mut hasher)
                            }
                            crate::events::redis_value::Kind::Error(e) => e.hash(&mut hasher),
                            _ => {} // Null/Array?
                        }
                    }
                }
            }
            Some(recorded_event::Event::PgMessage(msg)) => {
                "postgres".hash(&mut hasher);
                match &msg.message {
                    Some(crate::events::pg_message_event::Message::Query(q)) => {
                        q.query.hash(&mut hasher)
                    }
                    Some(crate::events::pg_message_event::Message::Parse(p)) => {
                        p.query.hash(&mut hasher);
                        // Ignore name for stability
                    }
                    Some(crate::events::pg_message_event::Message::Startup(s)) => {
                        s.user.hash(&mut hasher);
                        s.database.hash(&mut hasher);
                    }
                    Some(crate::events::pg_message_event::Message::Bind(_)) => {
                        "bind".hash(&mut hasher);
                        // Ignore portal and statement for stability
                    }
                    Some(crate::events::pg_message_event::Message::Execute(_)) => {
                        "execute".hash(&mut hasher);
                        // Ignore portal for stability
                    }
                    Some(crate::events::pg_message_event::Message::Sync(_)) => {
                        "sync".hash(&mut hasher);
                    }

                    _ => {} // Other PG messages?
                }
            }
            _ => {
                // Fallback or ignore?
                // If it's a response, we don't index it typically?
                // But recorder writes EVERYTHING.
                // We should probably explicitly return specific hash or 0 for non-requests.
                // BUT, looking at implementation plan: "Hash -> List<ResponseEvents>"
                // Typically we index the REQUEST.
                // So if it's a response, maybe we return 0 (no hash).
            }
        }

        hasher.finish()
    }
}
