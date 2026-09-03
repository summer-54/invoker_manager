use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Response, Status, codec::CompressionEncoding, metadata::MetadataMap};

use crate::{application::InvokersStreamsReceiver, prelude::*};

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

pub struct Server {
    pub map: Mutex<HashMap<Token, (Option<AS>, Option<MS>, Option<JS>, CertName)>>,
    pub sender: UnboundedSender<(AS, MS, JS, Token, CertName)>,
}

type ServiceStream<T> = UnboundedReceiverStream<Result<T, Status>>;

impl Server {
    pub async fn check(&self, token: &Token) -> Result<()> {
        let mut map = self.map.lock().await;
        if !matches!(map.get(token), Some((Some(_), Some(_), Some(_), ..))) {
            return Ok(());
        }

        let Some((token, (Some(auth_stream), Some(master_stream), Some(judge_stream), cert_name))) =
            map.remove_entry(token)
        else {
            unreachable!()
        };

        self.sender
            .send((auth_stream, master_stream, judge_stream, token, cert_name))?;
        Ok(())
    }
}

fn get_header_from_metadata<'a>(
    metadata: &'a MetadataMap,
    name: &str,
) -> Result<&'a str, tonic::Status> {
    metadata
        .get(name)
        .context("parsing TOKEN metadata")
        .map_err(|err| tonic::Status::invalid_argument(format!("{err:?}")))?
        .to_str()
        .context("converting TOKEN to string")
        .map_err(|err| tonic::Status::invalid_argument(format!("{err:?}")))
}

#[tonic::async_trait]
impl grpc::invoker_manager::Service for Server {
    type MasterStreamStream = ServiceStream<pb::MasterOutgo>;

    async fn master_stream(
        &self,
        request: tonic::Request<tonic::Streaming<pb::MasterIncome>>,
    ) -> Result<tonic::Response<Self::MasterStreamStream>, tonic::Status> {
        let (sender_outgo, receiver_outgo) =
            tokio::sync::mpsc::unbounded_channel::<Result<pb::MasterOutgo, Status>>();
        let metadata = request.metadata();
        let token: Token =
            get_header_from_metadata(metadata, grpc::invoker_manager::metadata::TOKEN)?.into();
        let cert_name: CertName =
            get_header_from_metadata(metadata, grpc::invoker_manager::metadata::CERT_NAME)?.into();

        let receiver = request.into_inner();

        let stream: ServerStream<pb::MasterIncome, pb::MasterOutgo, _> =
            grpc::ServerStream::new(receiver, sender_outgo);

        {
            self.map
                .lock()
                .await
                .entry(token.clone())
                .or_insert((None, None, None, cert_name))
                .1 = Some(stream);
        }

        self.check(&token)
            .await
            .map_err(|err| tonic::Status::internal(format!("{err:?}")))?;

        Ok(Response::new(ServiceStream::new(receiver_outgo)))
    }

    type JudgeStreamStream = ServiceStream<pb::JudgeOutgo>;

    async fn judge_stream(
        &self,
        request: tonic::Request<tonic::Streaming<pb::JudgeIncome>>,
    ) -> Result<tonic::Response<Self::JudgeStreamStream>, tonic::Status> {
        let (sender_outgo, receiver_outgo) =
            tokio::sync::mpsc::unbounded_channel::<Result<pb::JudgeOutgo, Status>>();

        let metadata = request.metadata();
        let token: Token =
            get_header_from_metadata(metadata, grpc::invoker_manager::metadata::TOKEN)?.into();
        let cert_name: CertName =
            get_header_from_metadata(metadata, grpc::invoker_manager::metadata::CERT_NAME)?.into();

        let receiver = request.into_inner();

        let stream: ServerStream<pb::JudgeIncome, pb::JudgeOutgo, _> =
            grpc::ServerStream::new(receiver, sender_outgo);

        {
            self.map
                .lock()
                .await
                .entry(token.clone())
                .or_insert((None, None, None, cert_name))
                .2 = Some(stream);
        }

        self.check(&token)
            .await
            .map_err(|err| tonic::Status::internal(format!("{err:?}")))?;

        Ok(Response::new(ServiceStream::new(receiver_outgo)))
    }

    type AuthStream = ServiceStream<pb::AuthOutgo>;

    async fn auth(
        &self,
        request: tonic::Request<tonic::Streaming<pb::AuthIncome>>,
    ) -> Result<tonic::Response<Self::AuthStream>, tonic::Status> {
        let (sender_outgo, receiver_outgo) =
            tokio::sync::mpsc::unbounded_channel::<Result<pb::AuthOutgo, Status>>();
        let metadata = request.metadata();
        let token: Token =
            get_header_from_metadata(metadata, grpc::invoker_manager::metadata::TOKEN)?.into();
        let cert_name: CertName =
            get_header_from_metadata(metadata, grpc::invoker_manager::metadata::CERT_NAME)?.into();

        let receiver = request.into_inner();

        let stream: ServerStream<pb::AuthIncome, pb::AuthOutgo, _> =
            grpc::ServerStream::new(receiver, sender_outgo);

        {
            self.map
                .lock()
                .await
                .entry(token.clone())
                .or_insert((None, None, None, cert_name))
                .0 = Some(stream);
        }

        self.check(&token)
            .await
            .map_err(|err| tonic::Status::internal(format!("{err:?}")))?;

        Ok(Response::new(ServiceStream::new(receiver_outgo)))
    }
}

pub struct ChannelReceiver {
    receiver: Mutex<UnboundedReceiver<(AS, MS, JS, Token, CertName)>>,
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
    ) -> impl std::future::Future<Output = Result<(Self::AS, Self::MS, Self::JS, Token, CertName)>>
    + Send
    + Sync
    + 'static {
        let this = self.clone();
        async move { this.receiver.lock().await.recv().await.context("closed") }
    }
}
