mod api;
mod application;
mod auth;
mod invoker;
mod prelude;
mod system;

use std::{env, str::FromStr, sync::Arc};

use http::Uri;
use prelude::*;
use toaster_lib_rs::server::websocket::Channel;

use std::net::SocketAddrV4;

use crate::application::App;

const INVOKER_GATE_SOCKET_ADDRESS_ENV: &str = "INVOKER_GATE_SOCKET_ADDRESS";
const SYSTEM_SOCKET_ADDRESS_ENV: &str = "SYSTEM_SOCKET_ADDRESS";
const AUTH_API_URL_ENV: &str = "AUTH_API_URL";

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let invoker_gate_socket_address: SocketAddrV4 = env::var(INVOKER_GATE_SOCKET_ADDRESS_ENV)
        .context(format!("{INVOKER_GATE_SOCKET_ADDRESS_ENV} env reading"))?
        .parse()
        .context(format!("{INVOKER_GATE_SOCKET_ADDRESS_ENV} parsing"))?;
    let system_socket_address: SocketAddrV4 = env::var(SYSTEM_SOCKET_ADDRESS_ENV)
        .context(format!("{SYSTEM_SOCKET_ADDRESS_ENV} env reading"))?
        .parse()
        .context(format!("{SYSTEM_SOCKET_ADDRESS_ENV} parsing"))?;

    let auth_api_url: reqwest::Url = env::var(AUTH_API_URL_ENV)
        .context(format!("{AUTH_API_URL_ENV} env reading"))?
        .parse()
        .context(format!("{AUTH_API_URL_ENV} parsing"))?;

    log::info!("starting with");

    let isr = invoker::server::websocket::ChannelReceiver::new(invoker_gate_socket_address).await?;
    let system_channel = Arc::new(
        Channel::bind(
            system_socket_address,
            Uri::from_str(format!("ws://{}", system_socket_address).as_str())?,
        )
        .await
        .context("binding system channel")?,
    );

    let system_master_stream = system_channel.new_stream(system::MASTER_NAME).await;

    let app = Arc::new(App {
        invokers_service: Arc::new(invoker::Service::default()),
        auth_service: Arc::new(auth::system_api::Service {
            api_url: auth_api_url,
        }),
    });

    app.run(Arc::new(isr), system_master_stream)
        .await
        .context("app run")?;

    Ok(())
}
