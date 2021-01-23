use std::collections::HashMap;

#[derive(Debug)]
pub struct ClusterState {
    pub replicas: HashMap<String, usize>,
}

pub trait ClusterStateFetcher {
    fn get_current_state(&self) -> anyhow::Result<ClusterState>;
}

struct SidekiqState {}

trait SidekiqStateFetcher {
    fn get_current_state() -> anyhow::Result<SidekiqState>;
}

struct Scaler<C: ClusterStateFetcher, S: SidekiqStateFetcher> {
    cluster_fetcher: C,
    sidekiq_fetcher: S,
}
