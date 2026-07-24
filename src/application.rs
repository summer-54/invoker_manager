use std::sync::Arc;

use crate::{auth, invoker, prelude::*, system};

use toaster_lib_rs::{judge::test, logger::LogState, server::stream::Stream};

pub trait InvokersStreamsReceiver {
    type AS: Stream<invoker::AuthIncome, invoker::AuthOutgo>;
    type MS: Stream<invoker::MasterIncome, invoker::MasterOutgo>;
    type JS: Stream<invoker::JudgeIncome, invoker::JudgeOutgo>;
    #[allow(clippy::type_complexity)]
    fn next(
        &self,
    ) -> impl std::future::Future<Output = Result<(Self::AS, Self::MS, Self::JS, Box<str>)>>
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
        AS: Stream<invoker::AuthIncome, invoker::AuthOutgo> + Send + 'static,
        MS: Stream<invoker::MasterIncome, invoker::MasterOutgo> + Send + Sync + 'static,
    >(
        self: Arc<Self>,
        auth_stream: AS,
        master_stream: MS,
        judge_stream: JS,
        id: Arc<str>,
    ) -> Result<()> {
        let invoker = self
            .clone()
            .invokers_service
            .create_invoker(auth_stream, &master_stream, judge_stream, id)
            .await?;
        let cert = self
            .auth_service
            .clone()
            .certificate(invoker.cert_name.clone())
            .await?;
        self.invokers_service.verify_invoker(invoker, cert).await?;

        Ok(())
    }
}
impl<
    JS: Stream<invoker::JudgeIncome, invoker::JudgeOutgo> + Send + Sync + 'static,
    AService: auth::Service,
> App<JS, AService>
{
    pub async fn run<
        ISR: InvokersStreamsReceiver<JS = JS> + Send + 'static,
        SMS: Stream<system::MasterIncome, system::MasterOutgo> + Send + 'static + Sync,
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
                let (auth_stream, master_stream, judge_stream, id) =
                    invoker_stream_receiver.next().await?;
                let id = Arc::from(id);
                let this = this.clone();
                tokio::spawn(async move {
                    let _ = this
                        .handle_invoker(auth_stream, master_stream, judge_stream, id)
                        .await
                        .context("handling invoker {id}")
                        .map_err(|err| {
                            log::error!("{err}");
                        });
                });
            }
        });

        let system = tokio::spawn(async move {
            let sms = Arc::new(system_master_stream);
            loop {
                match sms.recv().await? {
                    system::MasterIncome::Judge {
                        id,
                        test_count,
                        lang,
                        data,
                    } => {
                        let (sender, mut receiver) =
                            tokio::sync::mpsc::unbounded_channel::<test::ResultPayload>();

                        let sms_clone = sms.clone();
                        let handler = tokio::spawn(async move {
                            while let Some(payload) = receiver.recv().await {
                                let log_state =
                                    LogState::new().push("id", id).push("test_id", payload.id);
                                let _ = sms_clone
                                    .send(system::MasterOutgo::TestResult {
                                        id,
                                        test_id: payload.id,
                                        verdict: payload.result.verdict,
                                        data: payload.data,
                                    })
                                    .await
                                    .context("sending test result")
                                    .map_err(|e| log::error!("({log_state}) {e}"));
                            }
                            sms_clone
                        });

                        let result = self
                            .invokers_service
                            .judge_submission(test_count, lang, data, sender)
                            .await?;
                        let system_master_stream = handler.await?;
                        system_master_stream
                            .send(system::MasterOutgo::FullResult {
                                id,
                                verdict: result.result,
                                tests: result.tests,
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
