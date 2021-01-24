use crate::scaler::{SidekiqState, SidekiqStateFetcher};
use anyhow::Result;
use async_trait::async_trait;
use bb8_redis::{bb8::Pool, redis, redis::AsyncCommands, RedisConnectionManager};

pub struct Sidekiq {
    pool: Pool<RedisConnectionManager>,
}

impl Sidekiq {
    pub async fn new(c: impl redis::IntoConnectionInfo) -> Result<Sidekiq> {
        let manager = RedisConnectionManager::new(c)?;
        let pool = Pool::builder().build(manager).await?;
        Ok(Sidekiq { pool })
    }
}

#[async_trait]
impl SidekiqStateFetcher for Sidekiq {
    async fn get_current_state(&mut self) -> Result<SidekiqState> {
        let mut conn = self.pool.get().await?;
        let queues: Vec<String> = conn.smembers("sidekiq:queues").await?;

        let mut pipeline = redis::Pipeline::new();
        for queue in &queues {
            pipeline.cmd("LLEN").arg(format!("sidekiq:queue:{}", queue));
        }
        let lengths: Vec<usize> = pipeline.query_async(&mut *conn).await?;
        let queue_lengths = queues.into_iter().zip(lengths.into_iter()).collect();

        Ok(SidekiqState { queue_lengths })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nix::{sys::signal, unistd::Pid};
    use redis::{Commands, RedisResult};
    use signal::Signal::SIGTERM;
    use std::{panic::UnwindSafe, process::Command};

    fn with_redis_running<T>(port: usize, test: T) -> Result<()>
    where
        T: FnOnce() -> Result<()> + UnwindSafe,
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
    fn queue_lengths() -> Result<()> {
        let port = 31981;
        let url = format!("redis://localhost:{}/", port);

        with_redis_running(port, || {
            let mut conn = poll_for_connection(url.clone())?;
            conn.sadd("sidekiq:queues", "one-queue")?;
            conn.sadd("sidekiq:queues", "another-queue")?;
            conn.lpush("sidekiq:queue:another-queue", "one-job")?;
            conn.lpush("sidekiq:queue:another-queue", "another-job")?;

            let mut sidekiq = tokio_test::block_on(Sidekiq::new(url))?;
            let map = tokio_test::block_on(sidekiq.get_current_state())?;
            assert_eq!(map.queue_lengths.keys().len(), 2);
            assert_eq!(map.queue_lengths.get("one-queue"), Some(&0));
            assert_eq!(map.queue_lengths.get("another-queue"), Some(&2));

            Ok(())
        })
    }
}
