use toaster_lib_rs::auth::{Cert, CertName, Parse};

use crate::prelude::*;

pub struct Service;

impl super::Service for Service {
    async fn certificate(self: std::sync::Arc<Self>, cert_name: CertName) -> Result<Cert> {
        let v = tokio::fs::read(format!("./auth/{}.key", &*cert_name))
            .await
            .context("reading certificate")?;
        Cert::from_bytes(&v).context("reading from bytes")
    }
}
