//! Prints entity changes as they happen.
//!
//! HA_URL=http://homeassistant.local:8123 HA_TOKEN=… cargo run --example watch [entity_id…]

use ha_core::{EntityChange, HaClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let client = HaClient::connect(&std::env::var("HA_URL")?, &std::env::var("HA_TOKEN")?).await?;
    println!("connected to Home Assistant {}", client.ha_version());

    let filter: Vec<String> = std::env::args().skip(1).collect();
    let filter: Vec<&str> = filter.iter().map(String::as_str).collect();
    let mut watcher = client
        .watch_entities((!filter.is_empty()).then_some(&filter[..]))
        .await?;

    let first = watcher.next().await.ok_or("connection closed")??;
    println!("{} entities", first.len());
    while let Some(batch) = watcher.next().await {
        for change in batch? {
            match change {
                EntityChange::Added(s) => println!("+ {} = {}", s.entity_id, s.state),
                EntityChange::Updated(s) => {
                    println!("~ {} ({}) = {}", s.entity_id, s.name(), s.state)
                }
                EntityChange::Removed(id) => println!("- {id}"),
            }
        }
    }
    println!("connection closed");
    Ok(())
}
