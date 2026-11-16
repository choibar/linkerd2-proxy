//! One ownership choice for both tracing subscribers and datapath metrics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum WorkerScope {
    #[default]
    Shared,
    Worker,
}

impl WorkerScope {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value.trim() {
            "shared" => Ok(Self::Shared),
            "worker" => Ok(Self::Worker),
            other => Err(format!(
                "DPUMESH_L7_TRACE_SCOPE must be shared or worker, got {other:?}"
            )),
        }
    }

    pub fn from_env() -> Result<Self, String> {
        let sharded = std::env::var_os("DMESH_SHARDED").is_some();
        let value = match std::env::var("DPUMESH_L7_TRACE_SCOPE") {
            Ok(value) => value,
            Err(std::env::VarError::NotPresent) => if sharded { "worker" } else { "shared" }.into(),
            Err(error) => return Err(error.to_string()),
        };
        Self::for_sharded(&value, sharded)
    }

    pub fn for_sharded(value: &str, sharded: bool) -> Result<Self, String> {
        let scope = Self::parse(value)?;
        if scope == Self::Worker && !sharded {
            return Err("DPUMESH_L7_TRACE_SCOPE=worker requires DMESH_SHARDED".into());
        }
        Ok(scope)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_shared_configuration_for_both_tracing_and_metrics() {
        assert_eq!(
            WorkerScope::for_sharded("worker", true).unwrap(),
            WorkerScope::Worker
        );
        assert_eq!(
            WorkerScope::for_sharded(" shared ", false).unwrap(),
            WorkerScope::Shared
        );
        assert!(WorkerScope::for_sharded("worker", false).is_err());
        assert!(WorkerScope::for_sharded("private", true).is_err());
    }
}
