//! Toggles one entity.
//!
//! HA_URL=http://homeassistant.local:8123 HA_TOKEN=… cargo run --example toggle light.kitchen

use ha_core::HaClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let entity_id = std::env::args().nth(1).ok_or("usage: toggle <entity_id>")?;
    let client = HaClient::connect(&std::env::var("HA_URL")?, &std::env::var("HA_TOKEN")?).await?;
    client.toggle(&entity_id).await?;
    println!("toggled {entity_id}");
    Ok(())
}
