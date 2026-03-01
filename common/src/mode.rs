use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DejaMode {
    Record,
    Replay,
}

impl Default for DejaMode {
    fn default() -> Self {
        Self::Record
    }
}

impl std::fmt::Display for DejaMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Record => write!(f, "record"),
            Self::Replay => write!(f, "replay"),
        }
    }
}

impl FromStr for DejaMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "record" => Ok(Self::Record),
            "replay" => Ok(Self::Replay),
            _ => Err(format!("Invalid Deja mode: {}", s)),
        }
    }
}
