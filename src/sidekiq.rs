use redis::{Commands, RedisResult};
use std::collections::HashMap;

pub struct Sidekiq {
    connection: redis::Connection,
}

impl Sidekiq {
    pub fn new(c: impl redis::IntoConnectionInfo) -> RedisResult<Sidekiq> {
        Ok(Sidekiq {
            connection: redis::Client::open(c)?.get_connection()?,
        })
    }

    pub fn get_queue_lengths(&mut self) -> RedisResult<HashMap<String, usize>> {
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

            let mut sidekiq = Sidekiq::new(url)?;
            let map = sidekiq.get_queue_lengths()?;
            assert_eq!(map.keys().len(), 2);
            assert_eq!(map.get("one-queue"), Some(&0));
            assert_eq!(map.get("another-queue"), Some(&2));

            Ok(())
        })
    }
}
