use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Response, Status, Streaming, codec::CompressionEncoding, metadata::MetadataMap};

use crate::{application::InvokersStreamsReceiver, invoker::InvokerComponents, prelude::*};

use tokio::{
    sync::{
        Mutex,
        mpsc::{UnboundedReceiver, UnboundedSender},
    },
    task::JoinHandle,
};

use toaster_lib_rs::{
    auth::{CertName, Token},
    server::grpc::{self, ServerStream, pb::invoker_manager as pb},
};

type AS = grpc::ServerStream<pb::AuthIncome, pb::AuthOutgo, tonic::Streaming<pb::AuthIncome>>;
type MS = grpc::ServerStream<pb::MasterIncome, pb::MasterOutgo, tonic::Streaming<pb::MasterIncome>>;
type JS = grpc::ServerStream<pb::JudgeIncome, pb::JudgeOutgo, tonic::Streaming<pb::JudgeIncome>>;

struct InvokerBuilder {
    auth_stream: Option<AS>,
    master_stream: Option<MS>,
    judge_stream: Option<JS>,
    cert_name: CertName,
}

impl InvokerBuilder {
    pub fn new(cert_name: CertName) -> Self {
        Self {
            auth_stream: None,
            master_stream: None,
            judge_stream: None,
            cert_name,
        }
    }

    pub fn is_ready(&self) -> bool {
        self.auth_stream.is_some() && self.master_stream.is_some() && self.judge_stream.is_some()
    }
}

pub struct Server {
    map: Mutex<HashMap<Token, InvokerBuilder>>,
    sender: UnboundedSender<InvokerComponents<AS, MS, JS>>,
}

type ServiceStream<T> = UnboundedReceiverStream<Result<T, Status>>;

impl Server {
    pub async fn check(&self, token: &Token) -> Result<()> {
        let mut map = self.map.lock().await;
        if !matches!(map.get(token), Some(invoker_builder) if invoker_builder.is_ready()) {
            return Ok(());
        }

        let Some((
            token,
            InvokerBuilder {
                auth_stream: Some(auth_stream),
                master_stream: Some(master_stream),
                judge_stream: Some(judge_stream),
                cert_name,
            },
        )) = map.remove_entry(token)
        else {
            unreachable!()
        };

        self.sender.send(InvokerComponents {
            auth_stream,
            master_stream,
            judge_stream,
            token,
            cert_name,
        })?;
        Ok(())
    }
}

fn get_header_from_metadata<'a>(metadata: &'a MetadataMap, name: &str) -> Result<&'a str> {
    metadata
        .get(name)
        .context("parsing TOKEN metadata")?
        .to_str()
        .context("converting TOKEN to string")
}

async fn map_stream_request<
    I,
    O,
    H: AsyncFnOnce(MetadataMap, ServerStream<I, O, Streaming<I>>) -> Result<()>,
>(
    request: tonic::Request<tonic::Streaming<I>>,
    handler: H,
) -> Result<Response<ServiceStream<O>>, tonic::Status> {
    let (sender_outgo, receiver_outgo) =
        tokio::sync::mpsc::unbounded_channel::<Result<O, Status>>();
    let (metadata, _, receiver) = request.into_parts();
    let stream: ServerStream<I, O, _> = grpc::ServerStream::new(receiver, sender_outgo);

    handler(metadata, stream)
        .await
        .map_err(|err| Status::internal(format!("{err:?}")))?;

    Ok(Response::new(ServiceStream::new(receiver_outgo)))
}

#[tonic::async_trait]
impl grpc::invoker_manager::Service for Server {
    type MasterStreamStream = ServiceStream<pb::MasterOutgo>;

    async fn master_stream(
        &self,
        request: tonic::Request<tonic::Streaming<pb::MasterIncome>>,
    ) -> Result<tonic::Response<Self::MasterStreamStream>, tonic::Status> {
        map_stream_request(request, async |metadata, stream| {
            let token: Token =
                get_header_from_metadata(&metadata, grpc::invoker_manager::metadata::TOKEN)?.into();
            let cert_name: CertName =
                get_header_from_metadata(&metadata, grpc::invoker_manager::metadata::CERT_NAME)?
                    .into();

            {
                self.map
                    .lock()
                    .await
                    .entry(token.clone())
                    .or_insert(InvokerBuilder::new(cert_name))
                    .master_stream = Some(stream);
            };

            self.check(&token).await?;
            Ok(())
        })
        .await
    }

    type JudgeStreamStream = ServiceStream<pb::JudgeOutgo>;

    async fn judge_stream(
        &self,
        request: tonic::Request<tonic::Streaming<pb::JudgeIncome>>,
    ) -> Result<tonic::Response<Self::JudgeStreamStream>, tonic::Status> {
        map_stream_request(request, async |metadata, stream| {
            let token: Token =
                get_header_from_metadata(&metadata, grpc::invoker_manager::metadata::TOKEN)?.into();
            let cert_name: CertName =
                get_header_from_metadata(&metadata, grpc::invoker_manager::metadata::CERT_NAME)?
                    .into();
            {
                self.map
                    .lock()
                    .await
                    .entry(token.clone())
                    .or_insert(InvokerBuilder::new(cert_name))
                    .judge_stream = Some(stream);
            }

            self.check(&token).await?;
            Ok(())
        })
        .await
    }

    type AuthStream = ServiceStream<pb::AuthOutgo>;

    async fn auth(
        &self,
        request: tonic::Request<tonic::Streaming<pb::AuthIncome>>,
    ) -> Result<tonic::Response<Self::AuthStream>, tonic::Status> {
        map_stream_request(request, async |metadata, stream| {
            let token: Token =
                get_header_from_metadata(&metadata, grpc::invoker_manager::metadata::TOKEN)?.into();
            let cert_name: CertName =
                get_header_from_metadata(&metadata, grpc::invoker_manager::metadata::CERT_NAME)?
                    .into();
            {
                self.map
                    .lock()
                    .await
                    .entry(token.clone())
                    .or_insert(InvokerBuilder::new(cert_name))
                    .auth_stream = Some(stream);
            }

            self.check(&token)
                .await
                .map_err(|err| tonic::Status::internal(format!("{err:?}")))?;
            Ok(())
        })
        .await
    }
}

pub struct ChannelReceiver {
    receiver: Mutex<UnboundedReceiver<InvokerComponents<AS, MS, JS>>>,
    #[allow(unused)]
    server_handler: JoinHandle<Result<()>>,
}

impl ChannelReceiver {
    pub async fn new(socket_addr: SocketAddr) -> Result<ChannelReceiver> {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();

        let server = Server {
            map: Mutex::new(HashMap::new()),
            sender,
        };
        let server_handler = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(
                    grpc::invoker_manager::Server::new(server)
                        .accept_compressed(CompressionEncoding::Zstd)
                        .send_compressed(CompressionEncoding::Zstd)
                        .max_decoding_message_size(1024 * 1024 * 1024)
                        .max_encoding_message_size(1024 * 1024 * 1024),
                )
                .serve(socket_addr)
                .await
                .context("serve gRPC server")?;
            Ok(())
        });
        log::trace!("channel receiver TcpStream binded");
        Ok(ChannelReceiver {
            receiver: Mutex::new(receiver),
            server_handler,
        })
    }
}
impl InvokersStreamsReceiver for Arc<ChannelReceiver> {
    type AS = AS;

    type MS = MS;

    type JS = JS;

    fn next(
        &self,
    ) -> impl std::future::Future<Output = Result<InvokerComponents<Self::AS, Self::MS, Self::JS>>>
    + Send
    + Sync
    + 'static {
        let this = self.clone();
        async move { this.receiver.lock().await.recv().await.context("closed") }
    }
}
