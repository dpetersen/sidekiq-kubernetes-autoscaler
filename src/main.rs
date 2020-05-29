#[macro_use]
extern crate log;

mod config;
mod sidekiq;

use k8s_openapi::api::apps::v1::Deployment;
use kube::api::{Api, ListParams, Meta};
use kube::runtime::Reflector;
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
// TODO have to periodically run when the app has sidekiq-cron. Might have to store a key for when
// you last ran and ensure you run at some interval. But then you have to ensure you don't
// autoscale down until it's run for a minute or two. Also, you have to pick which deployment to
// run! Should be a required config!

#[tokio::main]
async fn main() -> anyhow::Result<()> {
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

    let mut sidekiq = Sidekiq::new("redis://127.0.0.1/")?;
    let queue_lengths = sidekiq.get_queue_lengths()?;
    dbg!(queue_lengths);

    let client = kube::Client::try_default().await?;
    let namespace = "default".to_string();
    let list: Api<Deployment> = Api::namespaced(client, &namespace);
    let params = ListParams::default()
        .timeout(10)
        .labels(&format!("app={}", application));
    let reflector = Reflector::new(list).params(params);

    tokio::spawn(keep_reflecting(reflector.clone()));

    reflector.run().await?;
    Ok(())
}

async fn keep_reflecting(reflector: Reflector<Deployment>) {
    loop {
        tokio::time::delay_for(std::time::Duration::from_secs(3)).await;
        let deployments: Vec<_> = reflector
            .state()
            .await
            .unwrap() // it returns a Result but can't error
            .iter()
            .filter(|o| {
                o.meta()
                    .name
                    .as_ref()
                    .unwrap_or(&"".to_string())
                    .contains("-sidekiq-")
            })
            .map(Meta::name)
            .collect();
        info!("current deploys: {:?}", deployments);
    }
}
