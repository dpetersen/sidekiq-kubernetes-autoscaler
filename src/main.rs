#[macro_use]
extern crate log;

mod config;
mod sidekiq;

use futures::{StreamExt, TryStreamExt};
use k8s_openapi::api::apps::v1::Deployment;
use kube::api::{Api, ListParams, Meta};
use kube_runtime::{reflector, reflector::store::Store, utils::try_flatten_applied, watcher};
use sidekiq::Sidekiq;
use structopt::StructOpt;

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
// say "oh there is no work to do" when sidekiq is still actually busy!
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

    let client = kube::Client::try_default().await?;
    let namespace = "default".to_string();
    let list: Api<Deployment> = Api::namespaced(client, &namespace);
    let params = ListParams::default()
        .timeout(10)
        .labels(&format!("app.kubernetes.io/instance={}", application));
    let store = reflector::store::Writer::<Deployment>::default();
    let readable = store.as_reader();
    let reflector = reflector(store, watcher(list, params));

    tokio::spawn(periodically_show_deployment_state(readable));

    let mut flattened_reflector = try_flatten_applied(reflector).boxed();
    while let Some(deployment) = flattened_reflector.try_next().await? {
        dbg!(deployment.name());
    }

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

async fn periodically_show_deployment_state(store: Store<Deployment>) {
    loop {
        let deployments: Vec<_> = store
            .state()
            .iter()
            .filter(|o| {
                o.meta()
                    .labels
                    .as_ref()
                    .unwrap_or(&std::collections::BTreeMap::new())
                    .get("app.kubernetes.io/component")
                    .unwrap_or(&"".to_string())
                    .starts_with("background-worker")
            })
            .map(Meta::name)
            .collect();
        dbg!(deployments);
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    }
}
