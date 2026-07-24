pub mod system_api;
use toaster_lib_rs::auth::Cert;

use crate::prelude::*;
use std::sync::Arc;

pub trait Service: Send + Sync + 'static {
    fn certificate(
        self: Arc<Self>,
        cert_name: Arc<str>,
    ) -> impl std::future::Future<Output = Result<Cert>> + Send;
}
