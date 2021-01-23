use crate::scaler::{ClusterState, ClusterStateFetcher};
use anyhow::{anyhow, Context};
use futures::{StreamExt, TryStreamExt};
use k8s_openapi::api::apps::v1::Deployment;
use kube::api::{Api, ListParams, Meta};
use kube_runtime::{
    reflector,
    reflector::store::{Store, Writer},
    utils::try_flatten_applied,
    watcher,
};
use std::collections::HashMap;
use tokio_util::sync::CancellationToken;

pub struct AppClusterStateFetcher {
    app: String,
    namespace: String,
}

impl AppClusterStateFetcher {
    pub fn new_for(app: String, namespace: String) -> (AppClusterStateFetcher, Writer<Deployment>) {
        let fetcher = AppClusterStateFetcher { app, namespace };
        let writer = reflector::store::Writer::<Deployment>::default();
        (fetcher, writer)
    }

    pub async fn start(
        self,
        store: Writer<Deployment>,
        cancel: CancellationToken,
    ) -> anyhow::Result<()> {
        let client = kube::Client::try_default().await?;
        let list: Api<Deployment> = Api::namespaced(client, &self.namespace);
        let params =
            ListParams::default().labels(&format!("app.kubernetes.io/instance={}", self.app));
        let reflector = reflector(store, watcher(list, params));
        let mut flattened_reflector = try_flatten_applied(reflector).boxed();

        debug!("deployment reflection started");
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    return Ok(());
                },
                next = flattened_reflector.try_next() => {
                    if let Err(e) = next {
                        return Err(anyhow!(e)).context("reflecting on deployments");
                    }
                },
            }
        }
    }
}

pub struct AppClusterStoreReader {
    reader: Store<Deployment>,
}

impl AppClusterStoreReader {
    pub fn new_with_store(reader: Store<Deployment>) -> AppClusterStoreReader {
        AppClusterStoreReader { reader }
    }
}

impl ClusterStateFetcher for AppClusterStoreReader {
    fn get_current_state(&self) -> anyhow::Result<ClusterState> {
        let replicas: HashMap<String, usize> = self
            .reader
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
            .map(|deployment| {
                // TODO no unwrap
                let name = deployment.metadata.name.as_ref().unwrap().clone();
                let replicas = deployment.spec.as_ref().unwrap().replicas.unwrap() as usize;
                (name, replicas)
            })
            .collect();

        Ok(ClusterState { replicas })
    }
}
