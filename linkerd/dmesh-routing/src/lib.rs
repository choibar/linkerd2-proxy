//! Validated DMA service membership shared by the proxy and its test controllers.
use serde::Deserialize;
use std::{
    collections::{BTreeMap, HashSet},
    net::SocketAddr,
    path::Path,
    sync::Arc,
};
use tokio::sync::watch;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct Manifest {
    pub schema_version: u32,
    pub generation: u64,
    pub workers: usize,
    pub services: BTreeMap<String, Service>,
}
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct Service {
    pub vip: std::net::Ipv4Addr,
    pub port: u16,
    pub discovery: String,
    pub endpoints: Vec<Endpoint>,
}
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct Endpoint {
    pub id: String,
    pub dma: SocketAddr,
    pub worker: usize,
    pub enabled: bool,
}
impl Manifest {
    pub fn read(path: &Path) -> Result<Self, String> {
        let m: Self = serde_json::from_slice(&std::fs::read(path).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;
        m.validate()?;
        Ok(m)
    }
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != 2
            || self.generation == 0
            || self.workers == 0
            || self.workers > 16
            || self.services.is_empty()
        {
            return Err("invalid routing schema/generation/workers/services".into());
        }
        let mut addresses = HashSet::new();
        let mut names = HashSet::new();
        let mut ids = HashSet::new();
        for s in self.services.values() {
            if s.port == 0
                || !addresses.insert(SocketAddr::from((s.vip, s.port)))
                || !names.insert(&s.discovery)
                || !s.discovery.ends_with(&format!(".dmesh:{}", s.port))
            {
                return Err("invalid/duplicate service address or discovery name".into());
            }
        }
        for s in self.services.values() {
            for e in &s.endpoints {
                if !e.dma.is_ipv4()
                    || e.dma.port() == 0
                    || e.worker >= self.workers
                    || e.id.is_empty()
                    || !addresses.insert(e.dma)
                    || !ids.insert(&e.id)
                {
                    return Err("invalid/duplicate endpoint or worker".into());
                }
            }
        }
        Ok(())
    }
    pub fn by_vip(&self, addr: SocketAddr) -> Option<&Service> {
        self.services
            .values()
            .find(|s| SocketAddr::from((s.vip, s.port)) == addr)
    }
    pub fn by_path(&self, path: &str) -> Option<&Service> {
        self.services.values().find(|s| s.discovery == path)
    }
    pub fn endpoint(&self, addr: SocketAddr) -> Option<&Endpoint> {
        self.services
            .values()
            .flat_map(|s| &s.endpoints)
            .find(|e| e.dma == addr)
    }
}

/// File replacement is the commit point. Invalid/stale updates keep the previous
/// snapshot. A thread avoids depending on whichever shard first uses the config.
pub fn from_env() -> Result<Option<watch::Receiver<Arc<Manifest>>>, String> {
    let Some(path) = std::env::var_os("DMESH_ROUTES") else {
        return Ok(None);
    };
    for flag in [
        "MOCK_POLICY_ECHO_TARGET",
        "MOCK_OUTBOUND_OPAQUE",
        "MOCK_DEST_OPAQUE",
    ] {
        if std::env::var_os(flag).is_some() {
            return Err(format!("DMESH_ROUTES conflicts with {flag}"));
        }
    }
    let path = std::path::PathBuf::from(path);
    let first = Manifest::read(&path)?;
    let (tx, rx) = watch::channel(Arc::new(first));
    std::thread::Builder::new()
        .name("dmesh-routes".into())
        .spawn(move || {
            let mut rejected = String::new();
            while tx.receiver_count() != 0 {
                std::thread::sleep(std::time::Duration::from_millis(200));
                let next = Manifest::read(&path).and_then(|m| {
                    let old = tx.borrow();
                    if m == **old {
                        return Ok(None);
                    }
                    if m.generation <= old.generation || m.workers != old.workers {
                        return Err(
                            "routing update needs a larger generation and unchanged workers".into(),
                        );
                    }
                    // Service identity is stable; only endpoint membership is live.
                    if m.services.len() != old.services.len()
                        || m.services.iter().any(|(k, s)| {
                            old.services.get(k).is_none_or(|o| {
                                (o.vip, o.port, &o.discovery) != (s.vip, s.port, &s.discovery)
                            })
                        })
                    {
                        return Err("service identity changes require restart".into());
                    }
                    for s in m.services.values() {
                        for e in &s.endpoints {
                            if old.endpoint(e.dma).is_some_and(|o| o.worker != e.worker) {
                                return Err(
                                    "moving an endpoint to a different worker requires restart"
                                        .into(),
                                );
                            }
                        }
                    }
                    Ok(Some(m))
                });
                match next {
                    Ok(Some(m)) => {
                        eprintln!("dmesh routes applied generation={}", m.generation);
                        tx.send_replace(Arc::new(m));
                        rejected.clear();
                    }
                    Ok(None) => {}
                    Err(e) if rejected != e => {
                        eprintln!("dmesh routes rejected: {e}");
                        rejected = e;
                    }
                    Err(_) => {}
                }
            }
        })
        .map_err(|e| e.to_string())?;
    Ok(Some(rx))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn manifest() -> Manifest {
        serde_json::from_str(r#"{"schema_version":2,"generation":1,"workers":4,"services":{"s":{"vip":"10.80.0.1","port":80,"discovery":"s.dmesh:80","endpoints":[{"id":"s0","dma":"10.81.1.1:80","worker":0,"enabled":true},{"id":"s1","dma":"10.81.1.2:80","worker":1,"enabled":true}]}}}"#).unwrap()
    }
    #[test]
    fn unique_addresses_and_workers() {
        let mut m = manifest();
        assert!(m.validate().is_ok());
        m.services.get_mut("s").unwrap().endpoints[1].dma = "10.80.0.1:80".parse().unwrap();
        assert!(m.validate().is_err());
        let mut m = manifest();
        m.services.get_mut("s").unwrap().endpoints[1].worker = 4;
        assert!(m.validate().is_err());
    }
    #[test]
    fn membership_is_not_readiness() {
        let mut m = manifest();
        m.services.get_mut("s").unwrap().endpoints[0].enabled = false;
        assert!(m.validate().is_ok());
        assert!(!m.endpoint("10.81.1.1:80".parse().unwrap()).unwrap().enabled);
        assert!(m.by_vip("10.80.0.1:80".parse().unwrap()).is_some());
        assert!(m.by_path("wrong.dmesh:80").is_none());
    }
}
