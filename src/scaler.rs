use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub struct ClusterState {
    pub replicas: HashMap<String, usize>,
}

pub trait ClusterStateFetcher {
    fn get_current_state(&self) -> Result<ClusterState>;
}

#[derive(Debug)]
pub struct SidekiqState {
    pub queue_lengths: HashMap<String, usize>,
}

#[async_trait]
pub trait SidekiqStateFetcher {
    async fn get_current_state(&mut self) -> Result<SidekiqState>;
}

pub struct Scaler<C: ClusterStateFetcher, S: SidekiqStateFetcher> {
    cluster_fetcher: C,
    sidekiq_fetcher: S,
}

impl<C: ClusterStateFetcher, S: SidekiqStateFetcher> Scaler<C, S> {
    pub fn new(cluster_fetcher: C, sidekiq_fetcher: S) -> Scaler<C, S> {
        Scaler {
            cluster_fetcher,
            sidekiq_fetcher,
        }
    }

    // TODO why can't this be a borrow? I suck at lifetimes and async.
    pub async fn run(mut self, cancel: CancellationToken) -> Result<()> {
        loop {
            // TODO the cluster state is empty until reflection has occurred. But the state doesn't
            // know if it's been written to yet...
            // TODO getting state probably shouldn't crash this function?
            let cluster_state = self.cluster_fetcher.get_current_state()?;
            if cluster_state.replicas.is_empty() {
                warn!("the cluster state is empty, so the reflector hasn't started or there are no pods for the application you've selected");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                continue;
            }
            // TODO if redis isn't running, this just... blocks forever. I set a timeout on bb8 but
            // it does nothing.
            let sidekiq_state = self.sidekiq_fetcher.get_current_state().await?;
            debug!("sleeping");

            tokio::select! {
                _ = cancel.cancelled() => {
                    debug!("Scaler run cancelled");
                    return Ok(());
                },
                _ = tokio::time::sleep(std::time::Duration::from_secs(3)) => {
                    debug!("waking");
                }
            }
        }
    }
}
