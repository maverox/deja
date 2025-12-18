use crate::events::{recorded_event::Event, RecordedEvent};
use serde_json::Value;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Mismatch {
    pub trace_id: String,
    pub diffs: Vec<DiffDetail>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DiffDetail {
    pub path: String,
    pub baseline: String,
    pub candidate: String,
}

#[derive(Debug, Clone, Default)]
pub struct NoiseConfig {
    /// Keys in JSON objects to ignore (e.g. "timestamp", "trace_id")
    pub json_ignored_keys: Vec<String>,
    /// Full paths to ignore (e.g. "http_request.headers.date")
    pub ignored_paths: Vec<String>,
}

pub struct DiffReport {
    pub mismatches: Vec<Mismatch>,
    pub total_compared: usize,
    pub total_mismatched: usize,
}

impl DiffReport {
    pub fn new() -> Self {
        Self {
            mismatches: Vec::new(),
            total_compared: 0,
            total_mismatched: 0,
        }
    }

    pub fn add_result(&mut self, mismatch: Option<Mismatch>) {
        self.total_compared += 1;
        if let Some(m) = mismatch {
            self.total_mismatched += 1;
            self.mismatches.push(m);
        }
    }
}

pub fn calculate_diff(
    baseline: &RecordedEvent,
    candidate: &RecordedEvent,
    config: &NoiseConfig,
) -> Option<Mismatch> {
    let mut diffs = Vec::new();

    // Compare generic fields if needed?
    // Usually trace_ids are different in replay vs recording, so maybe we ignore them or assume they match
    // by virtue of how we paired them. Use the baseline's trace_id for reporting.

    match (&baseline.event, &candidate.event) {
        (Some(Event::HttpRequest(b)), Some(Event::HttpRequest(c))) => {
            if b.method != c.method {
                diffs.push(DiffDetail {
                    path: "http_request.method".into(),
                    baseline: b.method.clone(),
                    candidate: c.method.clone(),
                });
            }
            if b.path != c.path {
                diffs.push(DiffDetail {
                    path: "http_request.path".into(),
                    baseline: b.path.clone(),
                    candidate: c.path.clone(),
                });
            }

            // Compare Headers
            for (k, v_b) in &b.headers {
                let path = format!("http_request.headers.{}", k);
                if is_ignored(&path, config) {
                    continue;
                }

                if let Some(v_c) = c.headers.get(k) {
                    if v_b != v_c {
                        diffs.push(DiffDetail {
                            path,
                            baseline: v_b.clone(),
                            candidate: v_c.clone(),
                        });
                    }
                } else {
                    diffs.push(DiffDetail {
                        path,
                        baseline: v_b.clone(),
                        candidate: "<missing>".into(),
                    });
                }
            }
            // Check for extra headers in candidate
            for k in c.headers.keys() {
                let path = format!("http_request.headers.{}", k);
                if is_ignored(&path, config) {
                    continue;
                }

                if !b.headers.contains_key(k) {
                    diffs.push(DiffDetail {
                        path,
                        baseline: "<missing>".into(),
                        candidate: c.headers.get(k).unwrap().clone(),
                    });
                }
            }

            // Body comparison
            compare_bodies(&b.body, &c.body, config, &mut diffs, "http_request.body");
        }
        (Some(Event::RedisCommand(b)), Some(Event::RedisCommand(c))) => {
            if b.command != c.command {
                diffs.push(DiffDetail {
                    path: "redis_command.command".into(),
                    baseline: b.command.clone(),
                    candidate: c.command.clone(),
                });
            }
            // Simplified args comparison
            if b.args.len() != c.args.len() {
                diffs.push(DiffDetail {
                    path: "redis_command.args.len".into(),
                    baseline: b.args.len().to_string(),
                    candidate: c.args.len().to_string(),
                });
            } else {
                // Deep comparison of args would go here, simplified for now
            }
        }
        (None, None) => {} // Both empty
        (Some(b_type), Some(c_type)) => {
            // Different event types!
            // Gets the name of the variant roughly
            diffs.push(DiffDetail {
                path: "event_type".into(),
                baseline: format!("{:?}", b_type), // Debug impl gives variant name
                candidate: format!("{:?}", c_type),
            });
        }
        (Some(_), None) => {
            diffs.push(DiffDetail {
                path: "event".into(),
                baseline: "Present".into(),
                candidate: "Missing".into(),
            });
        }
        (None, Some(_)) => {
            diffs.push(DiffDetail {
                path: "event".into(),
                baseline: "Missing".into(),
                candidate: "Present".into(),
            });
        }
    }

    if diffs.is_empty() {
        None
    } else {
        Some(Mismatch {
            trace_id: baseline.trace_id.clone(),
            diffs,
        })
    }
}

fn is_ignored(path: &str, config: &NoiseConfig) -> bool {
    config.ignored_paths.iter().any(|p| p == path)
}

fn compare_bodies(
    b: &[u8],
    c: &[u8],
    config: &NoiseConfig,
    diffs: &mut Vec<DiffDetail>,
    path_prefix: &str,
) {
    if b.is_empty() && c.is_empty() {
        return;
    }

    // Try JSON
    if let (Ok(val_b), Ok(val_c)) = (
        serde_json::from_slice::<Value>(b),
        serde_json::from_slice::<Value>(c),
    ) {
        compare_json(&val_b, &val_c, config, diffs, path_prefix);
    } else {
        if b != c {
            diffs.push(DiffDetail {
                path: path_prefix.into(),
                baseline: format!("<bytes len {}>", b.len()),
                candidate: format!("<bytes len {}>", c.len()),
            });
        }
    }
}

fn compare_json(
    b: &Value,
    c: &Value,
    config: &NoiseConfig,
    diffs: &mut Vec<DiffDetail>,
    path: &str,
) {
    if is_ignored(path, config) {
        return;
    }

    match (b, c) {
        (Value::Object(map_b), Value::Object(map_c)) => {
            for (k, v_b) in map_b {
                if config.json_ignored_keys.contains(k) {
                    continue;
                }

                let new_path = format!("{}.{}", path, k);
                if let Some(v_c) = map_c.get(k) {
                    compare_json(v_b, v_c, config, diffs, &new_path);
                } else {
                    diffs.push(DiffDetail {
                        path: new_path,
                        baseline: truncate_json(v_b),
                        candidate: "<missing>".into(),
                    });
                }
            }
            for (k, v_c) in map_c {
                if config.json_ignored_keys.contains(k) {
                    continue;
                }
                if !map_b.contains_key(k) {
                    let new_path = format!("{}.{}", path, k);
                    diffs.push(DiffDetail {
                        path: new_path,
                        baseline: "<missing>".into(),
                        candidate: truncate_json(v_c),
                    });
                }
            }
        }
        (Value::Array(arr_b), Value::Array(arr_c)) => {
            let max_len = std::cmp::max(arr_b.len(), arr_c.len());
            for i in 0..max_len {
                let new_path = format!("{}[{}]", path, i);
                let item_b = arr_b.get(i);
                let item_c = arr_c.get(i);

                match (item_b, item_c) {
                    (Some(vb), Some(vc)) => compare_json(vb, vc, config, diffs, &new_path),
                    (Some(vb), None) => {
                        diffs.push(DiffDetail {
                            path: new_path,
                            baseline: truncate_json(vb),
                            candidate: "<missing>".into(),
                        });
                    }
                    (None, Some(vc)) => {
                        diffs.push(DiffDetail {
                            path: new_path,
                            baseline: "<missing>".into(),
                            candidate: truncate_json(vc),
                        });
                    }
                    (None, None) => {}
                }
            }
        }
        (v_b, v_c) => {
            if v_b != v_c {
                diffs.push(DiffDetail {
                    path: path.into(),
                    baseline: truncate_json(v_b),
                    candidate: truncate_json(v_c),
                });
            }
        }
    }
}

fn truncate_json(v: &Value) -> String {
    let s = v.to_string();
    if s.len() > 50 {
        format!("{}...", &s[0..50])
    } else {
        s
    }
}
