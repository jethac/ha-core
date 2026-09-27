//! Registry entries, used to group entities into rooms and devices.
//!
//! Only the fields a dashboard needs are modelled; Home Assistant sends more.

use serde::Deserialize;

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct AreaEntry {
    pub area_id: String,
    pub name: String,
    #[serde(default)]
    pub icon: Option<String>,
    #[serde(default)]
    pub floor_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct DeviceEntry {
    pub id: String,
    #[serde(default)]
    pub area_id: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub name_by_user: Option<String>,
}

impl DeviceEntry {
    pub fn display_name(&self) -> Option<&str> {
        self.name_by_user.as_deref().or(self.name.as_deref())
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct EntityRegistryEntry {
    pub entity_id: String,
    #[serde(default)]
    pub area_id: Option<String>,
    #[serde(default)]
    pub device_id: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub icon: Option<String>,
    #[serde(default)]
    pub platform: Option<String>,
    #[serde(default)]
    pub hidden_by: Option<String>,
    #[serde(default)]
    pub disabled_by: Option<String>,
}

impl EntityRegistryEntry {
    pub fn is_visible(&self) -> bool {
        self.hidden_by.is_none() && self.disabled_by.is_none()
    }

    /// The entity's area, falling back to its device's area as Home Assistant does.
    pub fn effective_area<'a>(&'a self, devices: &'a [DeviceEntry]) -> Option<&'a str> {
        self.area_id.as_deref().or_else(|| {
            let device_id = self.device_id.as_deref()?;
            devices
                .iter()
                .find(|d| d.id == device_id)?
                .area_id
                .as_deref()
        })
    }
}
