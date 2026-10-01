//! Requested launch configuration, recorded as intent.
//!
//! Kelpie is harness-agnostic: it starts agents in Herdr and passes launch
//! arguments through. It never reads a harness's session stores, transcripts,
//! or server APIs to learn which model actually served a turn, so a requested
//! value is what the launch asked for and nothing more. Whether a backend
//! honored it is best effort and outside Kelpie's knowledge.

use serde::{Deserialize, Serialize};

/// Requested model, provider, or effort. Absence means nobody wrote the field.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct RequestedAttribution {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requested_json_omits_fields_nobody_wrote() {
        let requested = serde_json::to_value(RequestedAttribution::default()).expect("json");
        assert_eq!(requested, serde_json::json!({}));
    }
}
