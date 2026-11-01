use std::sync::Arc;

use crate::{
    auth,
    invoker::{self, InvokerComponents},
    prelude::*,
};

use toaster_lib_rs::{
    auth::{CertName, Token},
    judge::test,
    logger::LogState,
    server::stream::{Stream, testing_system},
};

pub trait InvokersStreamsReceiver {
    type AS: Stream<
            invoker::AuthIncome,
            invoker::AuthOutgo,
            AliveError: Send + Sync + 'static,
            BrokenError: Send + Sync + 'static,
        >;
    type MS: Stream<
            invoker::MasterIncome,
            invoker::MasterOutgo,
            AliveError: Send + Sync + 'static,
            BrokenError: Send + Sync + 'static,
        >;
    type JS: Stream<
            invoker::JudgeIncome,
            invoker::JudgeOutgo,
            AliveError: Send + Sync + 'static,
            BrokenError: Send + Sync + 'static,
        >;
    #[allow(clippy::type_complexity)]
    fn next(
        &self,
    ) -> impl std::future::Future<Output = Result<InvokerComponents<Self::AS, Self::MS, Self::JS>>>
    + Send
    + Sync
    + 'static;
}
pub struct App<JS: Stream<invoker::JudgeIncome, invoker::JudgeOutgo>, AService: auth::Service> {
    pub invokers_service: Arc<invoker::Service<JS>>,
    pub auth_service: Arc<AService>,
}
impl<JS: Stream<invoker::JudgeIncome, invoker::JudgeOutgo>, AService: auth::Service>
    App<JS, AService>
{
    async fn handle_invoker<
        AS: Stream<
                invoker::AuthIncome,
                invoker::AuthOutgo,
                AliveError: Send + Sync + 'static,
                BrokenError: Send + Sync + 'static,
            > + Send
            + 'static,
        MS: Stream<
                invoker::MasterIncome,
                invoker::MasterOutgo,
                AliveError: Send + Sync + 'static,
                BrokenError: Send + Sync + 'static,
            > + Send
            + Sync
            + 'static,
    >(
        self: Arc<Self>,
        auth_stream: AS,
        master_stream: MS,
        judge_stream: JS,
        cert_name: CertName,
        token: Token,
    ) -> Result<()> {
        let invoker = self
            .clone()
            .invokers_service
            .create_invoker(auth_stream, judge_stream, cert_name.clone(), token.clone())
            .await?;

        let cert = self
            .auth_service
            .clone()
            .certificate(invoker.cert_name.clone())
            .await?;
        self.invokers_service.verify_invoker(invoker, cert).await?;

        match master_stream.recv().await {
            Ok(
                toaster_lib_rs::server::stream::invoker_manager::master::InvokerToManager::Exited {
                    code,
                    ..
                },
            ) => {
                log::trace!("invoker {token:?} exited: with code: {code}");
            }

            Err(e) => {
                log::error!("recv master stream invoker {token:?} message: {e:?}");
            }
        }

        self.clone().invokers_service.delete_invoker(&token).await;

        Ok(())
    }
}
impl<
    JS: Stream<
            invoker::JudgeIncome,
            invoker::JudgeOutgo,
            AliveError: Send + Sync + 'static,
            BrokenError: Send + Sync + 'static,
        > + Send
        + Sync
        + 'static,
    AService: auth::Service,
> App<JS, AService>
{
    pub async fn run<
        ISR: InvokersStreamsReceiver<JS = JS> + Send + 'static,
        SMS: Stream<
                testing_system::SystemToManager,
                testing_system::ManagerToSystem,
                AliveError: Send + Sync + 'static,
                BrokenError: Send + Sync + 'static,
            >
            + Send
            + 'static
            + Sync,
    >(
        self: Arc<Self>,
        invoker_stream_receiver: ISR,
        system_master_stream: SMS,
    ) -> Result<()>
    where
        ISR::AS: Send + 'static,
        ISR::MS: Send + 'static + Sync,
    {
        let this = self.clone();
        let invokers = tokio::spawn(async move {
            loop {
                let InvokerComponents {
                    auth_stream,
                    master_stream,
                    judge_stream,
                    token,
                    cert_name,
                } = invoker_stream_receiver.next().await?;
                let this = this.clone();
                tokio::spawn(async move {
                    let _ = this
                        .handle_invoker(
                            auth_stream,
                            master_stream,
                            judge_stream,
                            cert_name,
                            token.clone(),
                        )
                        .await
                        .context(format!("handling invoker {token:?}"))
                        .map_err(|err| {
                            log::error!("{err:?}");
                        });
                });
            }
        });

        let system = tokio::spawn(async move {
            let sms = Arc::new(system_master_stream);
            loop {
                match sms
                    .recv()
                    .await
                    .context("recv master system message".to_string())?
                {
                    testing_system::SystemToManager::Judge {
                        submission_id,
                        test_count,
                        lang,
                        data,
                    } => {
                        let (sender, mut receiver) =
                            tokio::sync::mpsc::unbounded_channel::<test::ResultPayload>();

                        let sms_clone = sms.clone();
                        let submission_id_clone = submission_id.clone();
                        let handler = tokio::spawn(async move {
                            while let Some(payload) = receiver.recv().await {
                                let log_state = LogState::new()
                                    .push("submission id", format!("{submission_id_clone:?}"))
                                    .push("test_id", payload.id);
                                let _ = sms_clone
                                    .send(testing_system::ManagerToSystem::TestData {
                                        submission_id: submission_id_clone.clone(),
                                        test_id: payload.id,
                                        data: payload.data,
                                    })
                                    .await
                                    .context("sending judge result")
                                    .map_err(|e| log::error!("({log_state}) {e:?}"));
                            }
                            sms_clone
                        });

                        let result = self
                            .invokers_service
                            .judge_submission(test_count, lang, data, sender)
                            .await?;
                        let system_master_stream = handler.await?;
                        system_master_stream
                            .send(testing_system::ManagerToSystem::SubmissionResult {
                                submission_id: submission_id.clone(),
                                result,
                            })
                            .await?
                    }
                }
            }
        });

        tokio::select! {
            res = invokers => res.context("listening invokers")?,
            res = system => res.context("listening system")?,
        }
    }
}
