#[macro_use]
extern crate log;

mod cluster_state;
mod config;
mod scaler;
mod sidekiq;

use scaler::ClusterStateFetcher;
use sidekiq::Sidekiq;
use structopt::StructOpt;
use tokio_util::sync::CancellationToken;

#[derive(Debug, StructOpt)]
#[structopt(name = "sidekiq-kubernetes-autoscaler")]
struct Opt {
    /// The application short name to autoscale
    #[structopt(long)]
    application: String,
}

// TODO probably need to deal with redis-namespace!
// TODO Gonna need metrics, and that means a web server. See:
// https://github.com/clux/version-rs
// TODO this needs to take currently active work into account, not just queue size! Don't want to
// say "oh there is no work to do" when sidekiq is still actually busy! But "quiet" workers need to
// also be handled, since they are already shutting down. I *think* that means you can ignore them.
// TODO I originally considered having this thing run periodically to account for sidekiq-cron, but
// that's just crazy talk. If an app needs sidekiq-cron, it needs to run at least one worker
// permanently.

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // TODO load from file. Don't bother with auto-reloading, you can do that with Kubernetes and
    // ConfigMaps.
    // TODO don't ? out of transient errors
    let c = config::Config {
        deployments: vec![config::Deployment {
            name: "Test 1".to_string(),
            queues: vec!["queue-1".to_string(), "queue-2".to_string()],
            min_replicas: 0,
            max_replicas: 10,
        }],
        autoscaling: config::Autoscaling {
            max_jobs: vec![
                ("queue-1".to_string(), 100 as usize),
                ("queue-2".to_string(), 100 as usize),
            ]
            .into_iter()
            .collect(),
        },
    };
    c.replicas(std::collections::HashMap::new())?;

    let opt = Opt::from_args();
    let application = opt.application;
    env_logger::init();

    let sidekiq = Sidekiq::new("redis://127.0.0.1/").await?;
    tokio::spawn(periodically_show_sidekiq_state(sidekiq));

    let (cluster_state_fetcher, store) =
        cluster_state::AppClusterStateFetcher::new_for(application, "default".to_string());
    let cluster_store_reader =
        cluster_state::AppClusterStoreReader::new_with_store(store.as_reader());
    let cancel = CancellationToken::new();
    let state_fetch_result = tokio::spawn(cluster_state_fetcher.start(store, cancel.clone()));

    cluster_store_reader.get_current_state()?;

    tokio::signal::ctrl_c().await?;
    debug!("cancel requested");
    cancel.cancel();
    state_fetch_result.await??;

    info!("gracefully shut down");
    Ok(())
}

async fn periodically_show_sidekiq_state(mut sidekiq: Sidekiq) {
    loop {
        match sidekiq.get_queue_lengths().await {
            Ok(queues) => {
                dbg!(queues);
            }
            Err(e) => error!("getting sidekiq queue lengths: {}", e),
        };
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    }
}
