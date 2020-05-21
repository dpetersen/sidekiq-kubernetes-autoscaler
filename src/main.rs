#[macro_use]
extern crate log;

use k8s_openapi::api::apps::v1::Deployment;
use kube::api::{Api, ListParams, Meta};
use kube::runtime::Reflector;
use redis::{Commands, RedisResult};
use std::collections::HashMap;
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();
    let application = opt.application;
    env_logger::init();

    let mut sidekiq = Sidekiq::new_for_redis_url("redis://127.0.0.1/")?;
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

struct Sidekiq {
    connection: redis::Connection,
}

impl Sidekiq {
    fn new_for_redis_url(c: impl redis::IntoConnectionInfo) -> RedisResult<Sidekiq> {
        Ok(Sidekiq {
            connection: redis::Client::open(c)?.get_connection()?,
        })
    }

    fn get_queue_lengths(&mut self) -> RedisResult<HashMap<String, usize>> {
        let queues: Vec<String> = self.connection.smembers("sidekiq:queues")?;
        let lengths: Vec<usize> = queues
            .iter()
            .map(|s| format!("sidekiq:queue:{}", s))
            .map(|s| self.connection.llen(s))
            .collect::<RedisResult<Vec<usize>>>()?;
        let h = queues.into_iter().zip(lengths.into_iter()).collect();
        Ok(h)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nix::{sys::signal, unistd::Pid};
    use redis::Commands;
    use signal::Signal::SIGTERM;
    use std::{panic::UnwindSafe, process::Command};

    fn with_redis_running<T>(port: usize, test: T) -> anyhow::Result<()>
    where
        T: FnOnce() -> anyhow::Result<()> + UnwindSafe,
    {
        let mut handle = Command::new("redis-server")
            .args(&[
                "--save",
                r#""#,
                "--appendonly",
                "no",
                "--port",
                &port.to_string(),
            ])
            .stdout(std::process::Stdio::null())
            .spawn()
            .unwrap();

        let result = std::panic::catch_unwind(|| test());

        signal::kill(Pid::from_raw(handle.id() as i32), SIGTERM).unwrap();
        assert!(handle.wait().unwrap().success());

        assert!(result.is_ok());
        Ok(())
    }

    fn poll_for_connection(url: String) -> RedisResult<redis::Connection> {
        let client = redis::Client::open(url)?;

        let mut i = 0;
        loop {
            match client.get_connection() {
                Ok(c) => return Ok(c),
                Err(_) if i < 5 => {
                    std::thread::sleep(std::time::Duration::from_millis(100));
                    i += 1;
                }
                Err(e) => panic!("I give up trying to connect to the test redis: {}", e),
            }
        }
    }

    #[test]
    fn queue_lengths() -> anyhow::Result<()> {
        let port = 31981;
        let url = format!("redis://localhost:{}/", port);

        with_redis_running(port, || {
            let mut conn = poll_for_connection(url.clone())?;
            conn.sadd("sidekiq:queues", "one-queue")?;
            conn.sadd("sidekiq:queues", "another-queue")?;
            conn.lpush("sidekiq:queue:another-queue", "one-job")?;
            conn.lpush("sidekiq:queue:another-queue", "another-job")?;

            let mut sidekiq = Sidekiq::new_for_redis_url(url)?;
            let map = sidekiq.get_queue_lengths()?;
            assert_eq!(map.keys().len(), 2);
            assert_eq!(map.get("one-queue"), Some(&0));
            assert_eq!(map.get("another-queue"), Some(&2));

            Ok(())
        })
    }
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
