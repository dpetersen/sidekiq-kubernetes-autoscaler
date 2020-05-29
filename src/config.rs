use anyhow::anyhow;
use conv::*;
use std::collections::HashMap;

#[derive(Debug)]
pub struct Config {
    pub deployments: Vec<Deployment>,

    pub autoscaling: Autoscaling,
}

#[derive(Debug)]
pub struct Deployment {
    pub name: String,
    pub queues: Vec<String>,

    pub min_replicas: usize,
    pub max_replicas: usize,
}

impl Deployment {
    fn replicas_for_percentage(&self, percentage: f64) -> anyhow::Result<usize> {
        let mix_max_replica_difference = self.max_replicas - self.min_replicas;
        let scale_up_amount = f64::value_from(mix_max_replica_difference)? * percentage;
        let replicas = self.min_replicas + scale_up_amount.ceil() as usize;
        if replicas < self.min_replicas {
            return Ok(self.min_replicas);
        } else if replicas > self.max_replicas {
            return Ok(self.max_replicas);
        }
        Ok(replicas)
    }
}

#[derive(Debug)]
pub struct Autoscaling {
    pub max_jobs: HashMap<String, usize>,
}

impl Config {
    pub fn replicas(
        &self,
        queue_lengths: HashMap<String, usize>,
    ) -> anyhow::Result<HashMap<String, usize>> {
        let mut res = HashMap::new();
        for (queue, jobs) in queue_lengths {
            let deployments: Vec<_> = self
                .deployments
                .iter()
                .filter(|d| d.queues.contains(&queue))
                .collect();
            dbg!(&deployments);

            // you're failing here because I added an unexpected queue. It shouldn't actually fail
            // on this, so what should it do? Default to running max on any job and min on 0 jobs?
            let max_jobs = *self.autoscaling.max_jobs.get(&queue).ok_or(anyhow!(
                "no autoscaling configuration for queue '{}'",
                queue
            ))?;

            for d in deployments {
                let replicas =
                    d.replicas_for_percentage(f64::value_from(jobs)? / f64::value_from(max_jobs)?)?;

                res.entry(d.name.clone())
                    .and_modify(|i| {
                        if replicas > *i {
                            *i = replicas
                        }
                    })
                    .or_insert(replicas);
            }
        }
        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replicas_for_percentage() {
        let cases = vec![
            (0, 10, 1.0, 10),
            (0, 10, 3.0, 10),
            (0, 10, 0.0, 0),
            (1, 10, 0.0, 1),
            (0, 10, 0.1, 1),
            (0, 10, 0.01, 1),
        ];
        for (min_replicas, max_replicas, percentage, expected) in cases {
            let d = Deployment {
                name: "Test 1".to_string(),
                queues: vec!["queue-1".to_string(), "queue-2".to_string()],
                min_replicas,
                max_replicas,
            };
            assert_eq!(d.replicas_for_percentage(percentage).unwrap(), expected);
        }
    }

    #[test]
    fn replicas() -> anyhow::Result<()> {
        let c = Config {
            deployments: vec![
                Deployment {
                    name: "Test 1".to_string(),
                    queues: vec!["queue-1".to_string(), "queue-2".to_string()],
                    min_replicas: 0,
                    max_replicas: 10,
                },
                Deployment {
                    name: "Test 2".to_string(),
                    queues: vec!["queue-1".to_string()],
                    min_replicas: 0,
                    max_replicas: 20,
                },
                Deployment {
                    name: "Test 3".to_string(),
                    queues: vec!["queue-3".to_string()],
                    min_replicas: 0,
                    max_replicas: 10,
                },
            ],
            autoscaling: Autoscaling {
                max_jobs: vec![
                    ("queue-1".to_string(), 100 as usize),
                    ("queue-2".to_string(), 100 as usize),
                ]
                .into_iter()
                .collect(),
            },
        };

        let h = c.replicas(
            vec![
                ("queue-1".to_string(), 50),
                ("queue-2".to_string(), 0),
                ("unknown-queue".to_string(), 100),
            ]
            .into_iter()
            .collect(),
        )?;
        assert_eq!(h.len(), 2);
        assert_eq!(*h.get(&"Test 1".to_string()).unwrap(), 5 as usize);
        assert_eq!(*h.get(&"Test 2".to_string()).unwrap(), 10 as usize);

        Ok(())
    }

    // test queues that aren't covered in the autoscaling config (should I just error?)
}

// sidekiqAwareAutoscaling:
//   maxJobs:
//     high: 100
//     medium: 1000
//     low: 1000
// sidekiqs:
//   general:
//     sidekiqAwareAutoscaling:
//       min: 0
//       max: 10
//     queues:
//       - medium
//       - low
//   important:
//     sidekiqAwareAutoscaling:
//       min: 0
//       max: 5
//     queues:
//       - high
