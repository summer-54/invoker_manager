use std::sync::Arc;

use ratchet_deflate::{Compression, DeflateConfig, DeflateExtProvider};
use ratchet_rs::{SubprotocolRegistry, WebSocketConfig};

use crate::{application::InvokersStreamsReceiver, prelude::*};

use tokio::net::{TcpListener, ToSocketAddrs};

use super::stream;

use toaster_lib_rs::{
    logger::LogState,
    server::{
        RawMessage,
        websocket::{Channel, Stream},
    },
};

pub struct ChannelReceiver {
    listener: TcpListener,
}

const MAX_MESSAGE_SIZE: usize = 1 << 31;
const COMPRESSION_LEVEL: u32 = 9;

impl ChannelReceiver {
    pub async fn new<A: ToSocketAddrs>(socket_addr: A) -> Result<ChannelReceiver> {
        let listener = TcpListener::bind(socket_addr)
            .await
            .context("TcpStream binding")?;
        log::trace!("channel receiver TcpStream binded");
        Ok(ChannelReceiver { listener })
    }

    pub async fn next(&self) -> Result<(Channel, Box<str>)> {
        let (write, read, id) = loop {
            let mut log_state = LogState::new().push("channel receiver", "invoker");
            let (connection, address) = self.listener.accept().await.context("tcp connecting")?;
            match async {
                log_state = log_state.push("address", address);

                log::trace!("({log_state}) tcp connection accepted by address = {address}");
                log::trace!("({log_state}) trying ws upgrade");
                ratchet_rs::accept_with(
                    connection,
                    WebSocketConfig {
                        max_message_size: MAX_MESSAGE_SIZE,
                    },
                    DeflateExtProvider::with_config(DeflateConfig {
                        compression_level: Compression::new(COMPRESSION_LEVEL),
                        ..Default::default()
                    }),
                    SubprotocolRegistry::default(),
                )
                .await
                .context("ws connectiong")?
                .upgrade()
                .await
                .context("ws upgrading")?
                .into_websocket()
                .split()
                .context("ws splitting")
            }
            .await
            {
                Ok((write, read)) => {
                    break (write, read, address);
                }
                Err(e) => {
                    log::error!("({log_state}) {e:?}");
                }
            }
        };

        Ok((Channel::new(write, read), id.to_string().into()))
    }
}
impl InvokersStreamsReceiver for Arc<ChannelReceiver> {
    type AS = Stream<RawMessage>;

    type MS = Stream<RawMessage>;

    type JS = Stream<RawMessage>;

    fn next(
        &self,
    ) -> impl std::future::Future<Output = Result<(Self::AS, Self::MS, Self::JS, Box<str>)>>
    + Send
    + Sync
    + 'static {
        let this = self.clone();
        async move {
            let (channel, id) = (*this).next().await?;
            let channel = Arc::new(channel);
            let auth_stream = channel.new_stream(stream::AUTH_NAME).await;
            let master_stream = channel.new_stream(stream::MASTER_NAME).await;
            let judge_stream = channel.new_stream(stream::JUDGE_NAME).await;
            tokio::spawn(channel.run());
            Ok((auth_stream, master_stream, judge_stream, id))
        }
    }
}
