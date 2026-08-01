use serde::{Deserialize, Serialize};

/// Server-side statistics for a query, reported on every page.
///
/// Every field is informational: nothing in this crate reads one, and a
/// consumer that does is logging or displaying it. So the whole object is
/// `#[serde(default)]`, a producer that omits a counter, or a Trino release
/// that renames one, must not fail the query the statistics describe. A
/// missing counter reads as zero and a missing state as the empty string,
/// which is what "not reported" means for a value nothing branches on.
#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Stat {
    pub state: String,
    pub queued: bool,
    pub scheduled: bool,
    pub nodes: u32,
    pub total_splits: u32,
    pub queued_splits: u32,
    pub running_splits: u32,
    pub completed_splits: u32,
    pub cpu_time_millis: u64,
    pub wall_time_millis: u64,
    pub queued_time_millis: u64,
    pub elapsed_time_millis: u64,
    pub processed_rows: u64,
    pub processed_bytes: u64,
    pub peak_memory_bytes: u64,
    pub spilled_bytes: u64,
    #[serde(skip)] // TODO: remove this when StageStats is implemented
    pub root_stage: Option<StageStats>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StageStats {
    //TODO: impl this
}

#[cfg(test)]
mod tests {
    use super::*;

    // Every field here is reported for observability and read by nobody, so a
    // producer that omits one must not fail the query it belongs to. This also
    // covers Trino renaming or dropping a counter in a later release.
    #[test]
    fn stat_decodes_when_every_counter_is_absent() {
        let stat: Stat = serde_json::from_str(r#"{"state": "RUNNING"}"#)
            .expect("stats carrying only the state decode");
        assert_eq!(stat.state, "RUNNING");
        assert_eq!(stat.processed_rows, 0);
        assert_eq!(stat.elapsed_time_millis, 0);
        assert!(!stat.queued);
    }

    #[test]
    fn stat_decodes_when_it_is_empty() {
        let stat: Stat = serde_json::from_str("{}").expect("empty stats decode");
        assert_eq!(stat.state, "");
    }

    // An unrecognised counter must be ignored, not rejected: a newer
    // coordinator adds fields to this object.
    #[test]
    fn stat_ignores_an_unknown_field() {
        let stat: Stat = serde_json::from_str(r#"{"state": "FINISHED", "newCounter": 7}"#)
            .expect("an unknown counter is ignored");
        assert_eq!(stat.state, "FINISHED");
    }
}
