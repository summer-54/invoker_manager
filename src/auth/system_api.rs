use http::HeaderValue;
use reqwest::Url;
use toaster_lib_rs::auth::{Cert, Parse};

use crate::prelude::*;

const END_POINT: &str = "get_invoker_key";

pub struct Service {
    pub api_url: Url,
}

impl super::Service for Service {
    async fn certificate(
        self: std::sync::Arc<Self>,
        cert_name: std::sync::Arc<str>,
    ) -> Result<Cert> {
        log::trace!("Trying to get authorise key '{cert_name}' from api");
        let mut request =
            reqwest::Request::new(reqwest::Method::GET, self.api_url.join(END_POINT)?);
        let _ = request.headers_mut().insert(
            "Authorization",
            HeaderValue::from_str(&cert_name).context("creating header")?,
        );
        let client = reqwest::Client::new();
        let response = client
            .execute(request)
            .await
            .context("executing request")?
            .error_for_status()
            .context("status was converted into error")?;
        let bytes = response.bytes().await.context("getting response bytes")?;
        Cert::from_bytes(&bytes).context("converting bytes into certificate")
    }
}
