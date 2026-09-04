mod application;
mod auth;
mod invoker;
mod prelude;

use std::{env, sync::Arc};

use prelude::*;

use std::net::SocketAddrV4;

use crate::application::App;

const INVOKER_GATE_SOCKET_ADDRESS_ENV: &str = "INVOKER_GATE_SOCKET_ADDRESS";
#[cfg(not(feature = "mock"))]
const SYSTEM_SOCKET_ADDRESS_ENV: &str = "SYSTEM_SOCKET_ADDRESS";
#[cfg(not(feature = "mock"))]
const AUTH_API_URL_ENV: &str = "AUTH_API_URL";

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let invoker_gate_socket_address: SocketAddrV4 = env::var(INVOKER_GATE_SOCKET_ADDRESS_ENV)
        .context(format!("{INVOKER_GATE_SOCKET_ADDRESS_ENV} env reading"))?
        .parse()
        .context(format!("{INVOKER_GATE_SOCKET_ADDRESS_ENV} parsing"))?;
    #[cfg(not(feature = "mock"))]
    let system_socket_address: SocketAddrV4 = env::var(SYSTEM_SOCKET_ADDRESS_ENV)
        .context(format!("{SYSTEM_SOCKET_ADDRESS_ENV} env reading"))?
        .parse()
        .context(format!("{SYSTEM_SOCKET_ADDRESS_ENV} parsing"))?;
    #[cfg(not(feature = "mock"))]
    let auth_api_url: reqwest::Url = env::var(AUTH_API_URL_ENV)
        .context(format!("{AUTH_API_URL_ENV} env reading"))?
        .parse()
        .context(format!("{AUTH_API_URL_ENV} parsing"))?;

    log::info!("starting with");

    let isr =
        invoker::server::grpc::ChannelReceiver::new(invoker_gate_socket_address.into()).await?;
    #[cfg(not(feature = "mock"))]
    let (system_stream, auth_service) = {
        use toaster_lib_rs::server::grpc::{self, ClientStream};
        use tonic::{codec::CompressionEncoding, transport::Channel};

        let channel = Channel::builder(
            format!("ws://{}/api/setup", system_socket_address)
                .parse()
                .context("parsing manager host field")?,
        )
        .connect()
        .await?;

        let mut client = grpc::testing_system::Client::new(channel)
            .accept_compressed(CompressionEncoding::Zstd)
            .send_compressed(CompressionEncoding::Zstd)
            .max_decoding_message_size(1024 * 1024 * 1024)
            .max_encoding_message_size(1024 * 1024 * 1024);
        let stream = ClientStream::from_fn(async |req| client.stream(req).await).await?;
        (
            stream,
            auth::system_api::Service {
                api_url: auth_api_url,
            },
        )
    };
    #[cfg(feature = "mock")]
    let (system_stream, auth_service) = {
        let sms_logger = |msg| {
            log::trace!("sending msg into system master stream: {msg:?}");
        };

        let system_master_stream = toaster_lib_rs::server::stream::mock::Mock::new(sms_logger);
        (system_master_stream, auth::mock::Service {})
    };
    let app = Arc::new(App {
        invokers_service: Arc::new(invoker::Service::default()),
        auth_service: Arc::new(auth_service),
    });

    app.run(Arc::new(isr), system_stream)
        .await
        .context("app run")?;

    Ok(())
}
