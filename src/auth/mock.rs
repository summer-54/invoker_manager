use reqwest::Url;
use toaster_lib_rs::auth::{Cert, Parse};

use crate::prelude::*;

pub struct Service {
    pub api_url: Url,
}

impl super::Service for Service {
    async fn certificate(
        self: std::sync::Arc<Self>,
        cert_name: std::sync::Arc<str>,
    ) -> Result<Cert> {
        let v = tokio::fs::read(format!("./auth/{cert_name}.pub"))
            .await
            .context("reading certificate")?;
        Cert::from_bytes(&v).context("reading from bytes")
    }
}
