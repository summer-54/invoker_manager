pub mod server;

use crate::{api::SubmissionResult, prelude::*};

use std::{collections::HashMap, ops::Deref, sync::Arc};

use tokio::sync::{Mutex, mpsc::UnboundedSender};
use uuid::Uuid;

const CHALLENGE_SIZE: usize = 128;

pub use server::stream::{
    AuthIncome, AuthOutgo, JudgeIncome, JudgeOutgo, MasterIncome, MasterOutgo,
};
use toaster_lib_rs::{
    auth::{Cert, Challenge, policy},
    judge::{Lang, submission, test},
    logger::LogState,
    poll::ResourcePool,
    server::stream::Stream,
};

struct InvokerGuard<JS: Stream<JudgeIncome, JudgeOutgo> + Send + Sync + 'static> {
    service: Arc<Service<JS>>,
    invoker: Arc<Invoker<JS>>,
    uuid: Uuid,
}

impl<JS: Stream<JudgeIncome, JudgeOutgo> + Send + Sync + 'static> Deref for InvokerGuard<JS> {
    type Target = Invoker<JS>;

    fn deref(&self) -> &Self::Target {
        &self.invoker
    }
}

impl<JS: Stream<JudgeIncome, JudgeOutgo> + Send + Sync + 'static> Drop for InvokerGuard<JS> {
    fn drop(&mut self) {
        let service = self.service.clone();
        let uuid = self.uuid;
        tokio::spawn(async move {
            service.poll.put(uuid);
            log::trace!("invoker {uuid} returned to pool");
        });
    }
}

pub struct Service<JS: Stream<JudgeIncome, JudgeOutgo>> {
    invokers: Mutex<HashMap<Uuid, Arc<Invoker<JS>>>>,
    pub(self) poll: ResourcePool<Uuid>,
}

impl<JS: Stream<JudgeIncome, JudgeOutgo>> Default for Service<JS> {
    fn default() -> Self {
        Self {
            invokers: Default::default(),
            poll: Default::default(),
        }
    }
}

impl<JS: Stream<JudgeIncome, JudgeOutgo>> Service<JS> {
    pub async fn create_invoker<
        AS: Stream<AuthIncome, AuthOutgo>,
        MS: Stream<MasterIncome, MasterOutgo>,
    >(
        &self,
        auth_stream: AS,
        master_stream: &MS,
        judge_stream: JS,
        id: Arc<str>,
    ) -> Result<UnverifiedInvoker<AS, JS>> {
        UnverifiedInvoker::new(auth_stream, master_stream, judge_stream, id.clone())
            .await
            .context(format!("creating invoker {id}"))
    }

    pub async fn verify_invoker(
        &self,
        invoker: UnverifiedInvoker<impl Stream<AuthIncome, AuthOutgo>, JS>,
        cert: Cert,
    ) -> Result<()> {
        let invoker = invoker.verify(cert).await?;
        let id = invoker.token;
        self.invokers
            .lock()
            .await
            .insert(invoker.token, Arc::new(invoker));
        self.poll.put(id);
        Ok(())
    }
}

impl<JS: Stream<JudgeIncome, JudgeOutgo> + Send + Sync + 'static> Service<JS> {
    async fn take_invoker(self: &Arc<Self>) -> InvokerGuard<JS> {
        let uuid = self.poll.take().await;
        InvokerGuard {
            service: self.clone(),
            invoker: self.invokers.lock().await[&uuid].clone(),
            uuid,
        }
    }

    pub async fn judge_submission(
        self: &Arc<Self>,
        test_count: usize,
        lang: Lang,
        submission: Box<[u8]>,
        sender: UnboundedSender<test::ResultPayload>,
    ) -> Result<SubmissionResult> {
        let invoker = self.take_invoker().await;
        invoker
            .judge_submission(test_count, lang, submission, sender)
            .await
            .context("testing submission")
    }
}

pub struct Invoker<JS: Stream<JudgeIncome, JudgeOutgo>> {
    judge_stream: JS,
    pub token: Uuid,
}

impl<JS: Stream<JudgeIncome, JudgeOutgo>> Invoker<JS> {
    pub async fn judge_submission(
        &self,
        test_count: usize,
        lang: Lang,
        submission: Box<[u8]>,
        sender: UnboundedSender<test::ResultPayload>,
    ) -> Result<SubmissionResult> {
        let log_state = LogState::new().push("invoker", self.token);
        log::trace!("({log_state}) start testing on invoker");
        self.judge_stream
            .send(JudgeOutgo::Run {
                lang,
                data: submission,
            })
            .await?;
        let mut verdicts = vec![None; test_count].into_boxed_slice();

        let submission_result = loop {
            match match self.judge_stream.recv().await.context("recv judge stream") {
                Ok(msg) => msg,
                Err(e) => {
                    log::error!("({log_state}) {e}");
                    continue;
                }
            } {
                JudgeIncome::FullResult(result) => {
                    break result;
                }
                JudgeIncome::TestResult(payload) => {
                    verdicts[payload.id] = Some(payload.result.clone());
                    sender
                        .send(payload)
                        .context("internal mspc channel sending test payload")?;
                }
                JudgeIncome::Error { msg } => {
                    log::error!("({log_state}) error: judging: {msg}");
                    break submission::Result::Te(msg);
                }
                JudgeIncome::OpError { msg } => {
                    log::error!("({log_state}) op_error: judging: {msg}");
                    break submission::Result::Te(msg);
                }
            }
        };
        log::trace!("({log_state}) testing end on invoker");

        Ok(SubmissionResult {
            result: submission_result,
            tests: verdicts,
        })
    }
}

pub struct UnverifiedInvoker<AS: Stream<AuthIncome, AuthOutgo>, JS: Stream<JudgeIncome, JudgeOutgo>>
{
    invoker: Invoker<JS>,
    pub cert_name: Arc<str>,
    pub auth_stream: AS,
}

impl<AS: Stream<AuthIncome, AuthOutgo>, JS: Stream<JudgeIncome, JudgeOutgo>>
    UnverifiedInvoker<AS, JS>
{
    pub async fn new<MS: Stream<MasterIncome, MasterOutgo>>(
        auth_stream: AS,
        master_stream: &MS,
        judge_stream: JS,
        id: Arc<str>,
    ) -> Result<Self> {
        let (token, cert_name) = loop {
            match master_stream.recv().await.context("recv token message") {
                Ok(MasterIncome::Token { token, name }) => break (token, name),
                Ok(_) => log::warn!("invoker {id} sended message, but not sended token"),
                Err(e) => {
                    log::error!("can't read message: {e}");
                }
            }
        };
        Ok(UnverifiedInvoker {
            invoker: Invoker {
                judge_stream,
                token,
            },
            auth_stream,
            cert_name: Arc::from(cert_name),
        })
    }

    pub async fn verify(self, cert: Cert) -> Result<Invoker<JS>> {
        let challenge = Challenge::generate(CHALLENGE_SIZE, &mut rand::rng());
        self.auth_stream
            .send(AuthOutgo::Challenge(challenge.clone()))
            .await?;

        let AuthIncome::ChallengeSolution(solution) = loop {
            match self.auth_stream.recv().await.context("reading auth stream") {
                Ok(msg) => break msg,
                Err(e) => log::error!("{e}"),
            };
        };

        solution.verify(&challenge, &cert, &policy::StandardPolicy::new())?;
        Ok(self.invoker)
    }
}
