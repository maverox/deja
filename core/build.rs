use std::io::Result;

fn main() -> Result<()> {
    prost_build::Config::new()
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .field_attribute("metadata", "#[serde(default)]")
        .compile_protos(&["../proto/deja/v1/events.proto"], &["../proto/"])?;
    Ok(())
}
