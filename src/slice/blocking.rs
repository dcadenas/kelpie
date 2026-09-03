//! The synchronous Herdr home used by the facade and deterministic inline tests.
//!
//! The daemon poll path must use `HerdrExec` instead. Keeping direct client
//! access here makes an accidental accept-thread wait mechanically visible.

use crate::herdr::{AgentObservation, HerdrConnection, HerdrError, LifecycleObservation, Snapshot};

use super::Kelpie;

impl Kelpie {
    pub(super) fn blocking_negotiate(&self) -> Result<(), HerdrError> {
        self.herdr.negotiate()
    }

    pub(super) fn blocking_snapshot(&self) -> Result<Snapshot, HerdrError> {
        self.herdr.snapshot()
    }

    pub(super) fn blocking_lifecycle_snapshot(
        &self,
    ) -> Result<Vec<LifecycleObservation>, HerdrError> {
        self.herdr.lifecycle_snapshot()
    }

    pub(super) fn blocking_connect(&self) -> Result<HerdrConnection, HerdrError> {
        self.herdr.connect()
    }

    pub(super) fn blocking_close_pane(
        &self,
        request_id: &str,
        pane_id: &str,
    ) -> Result<(), HerdrError> {
        self.herdr.close_pane(request_id, pane_id)
    }

    pub(super) fn blocking_agent(
        &self,
        request_id: &str,
        target: &str,
    ) -> Result<AgentObservation, HerdrError> {
        self.herdr.agent(request_id, target)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn direct_herdr_access_stays_in_the_blocking_home() {
        let facade = include_str!("../slice.rs");
        assert!(
            !facade.contains("self.herdr."),
            "direct Herdr calls in slice.rs bypass the designated blocking home"
        );
    }
}
