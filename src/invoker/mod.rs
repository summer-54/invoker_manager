pub mod server;

use crate::prelude::*;

use std::{collections::HashMap, ops::Deref, sync::Arc};

use tokio::sync::{Mutex, mpsc::UnboundedSender};

const CHALLENGE_SIZE: usize = 128;

pub use server::stream::{
    AuthIncome, AuthOutgo, JudgeIncome, JudgeOutgo, MasterIncome, MasterOutgo,
};
use toaster_lib_rs::{
    auth::{Cert, CertName, Challenge, Token, policy},
    judge::{
        Lang,
        submission::{self, FullResult},
        test,
    },
    logger::LogState,
    poll::ResourcePool,
    server::stream::Stream,
};

pub struct InvokerComponents<AS, MS, JS>
where
    AS: Stream<AuthIncome, AuthOutgo>,
    JS: Stream<JudgeIncome, JudgeOutgo>,
    MS: Stream<MasterIncome, MasterOutgo>,
{
    pub auth_stream: AS,
    pub master_stream: MS,
    pub judge_stream: JS,
    pub cert_name: CertName,
    pub token: Token,
}

struct InvokerGuard<JS: Stream<JudgeIncome, JudgeOutgo> + Send + Sync + 'static> {
    service: Arc<Service<JS>>,
    invoker: Arc<Invoker<JS>>,
    token: Token,
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
        let token = self.token.clone();
        tokio::spawn(async move {
            service.poll.put(token.clone());
            log::trace!("invoker {token:?} returned to pool");
        });
    }
}

pub struct Service<JS: Stream<JudgeIncome, JudgeOutgo>> {
    invokers: Mutex<HashMap<Token, Arc<Invoker<JS>>>>,
    pub(self) poll: ResourcePool<Token>,
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
    pub async fn create_invoker<AS: Stream<AuthIncome, AuthOutgo>>(
        &self,
        auth_stream: AS,
        judge_stream: JS,
        cert_name: CertName,
        token: Token,
    ) -> Result<UnverifiedInvoker<AS, JS>> {
        UnverifiedInvoker::new(auth_stream, judge_stream, cert_name.clone(), token.clone())
            .await
            .context(format!("creating invoker {cert_name:?}:{token:?}"))
    }

    pub async fn verify_invoker(
        &self,
        invoker: UnverifiedInvoker<impl Stream<AuthIncome, AuthOutgo>, JS>,
        cert: Cert,
    ) -> Result<()> {
        let invoker = invoker.verify(cert).await?;
        let token = invoker.token.clone();
        self.invokers
            .lock()
            .await
            .insert(token.clone(), Arc::new(invoker));
        log::info!("added new invoker: {token:?}");
        self.poll.put(token);
        Ok(())
    }

    pub async fn delete_invoker(&self, token: &Token) {
        log::info!("delete invoker '{token:?}'");
        self.invokers.lock().await.remove(token);
    }

    pub async fn check_invoker(&self, token: &Token) -> bool {
        self.invokers.lock().await.contains_key(token)
    }
}

impl<JS: Stream<JudgeIncome, JudgeOutgo> + Send + Sync + 'static> Service<JS> {
    async fn take_invoker(self: &Arc<Self>) -> InvokerGuard<JS> {
        let token = loop {
            let token = self.poll.take().await;
            if self.check_invoker(&token).await {
                break token;
            }
        };
        InvokerGuard {
            service: self.clone(),
            invoker: self.invokers.lock().await[&token].clone(),
            token,
        }
    }

    pub async fn judge_submission(
        self: &Arc<Self>,
        test_count: usize,
        lang: Lang,
        submission: Box<[u8]>,
        sender: UnboundedSender<test::ResultPayload>,
    ) -> Result<FullResult> {
        let invoker = self.take_invoker().await;
        invoker
            .judge_submission(test_count, lang, submission, sender)
            .await
            .context("testing submission")
    }
}

pub struct Invoker<JS: Stream<JudgeIncome, JudgeOutgo>> {
    judge_stream: JS,
    pub token: Token,
}

impl<JS: Stream<JudgeIncome, JudgeOutgo>> Invoker<JS> {
    pub async fn judge_submission(
        &self,
        test_count: usize,
        lang: Lang,
        submission: Box<[u8]>,
        sender: UnboundedSender<test::ResultPayload>,
    ) -> Result<FullResult> {
        let log_state = LogState::new().push("invoker", self.token.to_string());
        log::trace!("({log_state}) start testing on invoker");
        self.judge_stream
            .send(JudgeOutgo::Run {
                lang,
                data: submission,
            })
            .await?;
        let mut results = vec![None; test_count].into_boxed_slice();

        let submission_result = loop {
            match match self
                .judge_stream
                .recv()
                .await
                .context("recv judge stream")?
                .context("recv judge stream")
            {
                Ok(msg) => msg,
                Err(e) => {
                    log::error!("({log_state}) {e:?}");
                    continue;
                }
            } {
                JudgeIncome::SubmissionResult(result) => {
                    break result;
                }
                JudgeIncome::TestResultPayload(payload) => {
                    results[payload.id] = Some(payload.result.clone());
                    sender
                        .send(payload)
                        .context("internal mspc channel sending test payload")?;
                }
                JudgeIncome::Error(msg) => {
                    log::error!("({log_state}) error: judging: {msg:?}");
                    break submission::Result::Te(msg.into_inner());
                }
            }
        };
        log::trace!("({log_state}) testing end on invoker");

        Ok(match submission_result {
            submission::ResultWrapper::Ok {
                score,
                group_scores,
                ..
            } => FullResult::Ok {
                score,
                group_scores,
                value: results,
            },
            submission::ResultWrapper::Ce(msg) => FullResult::Ce(msg),
            submission::ResultWrapper::Te(msg) => FullResult::Te(msg),
        })
    }
}

pub struct UnverifiedInvoker<AS: Stream<AuthIncome, AuthOutgo>, JS: Stream<JudgeIncome, JudgeOutgo>>
{
    invoker: Invoker<JS>,
    pub cert_name: CertName,
    pub auth_stream: AS,
}

impl<AS: Stream<AuthIncome, AuthOutgo>, JS: Stream<JudgeIncome, JudgeOutgo>>
    UnverifiedInvoker<AS, JS>
{
    pub async fn new(
        auth_stream: AS,
        judge_stream: JS,
        cert_name: CertName,
        token: Token,
    ) -> Result<Self> {
        Ok(UnverifiedInvoker {
            invoker: Invoker {
                judge_stream,
                token,
            },
            auth_stream,
            cert_name,
        })
    }

    pub async fn verify(self, cert: Cert) -> Result<Invoker<JS>> {
        let log_state = LogState::new().push(
            "token",
            format!("{:?}:{:?}", self.cert_name, self.invoker.token),
        );
        let challenge = Challenge::generate(CHALLENGE_SIZE, &mut rand::rng());
        self.auth_stream
            .send(AuthOutgo::Challenge(challenge.clone()))
            .await?;

        let solution = loop {
            match self
                .auth_stream
                .recv()
                .await
                .context("reading auth stream")?
                .context("reading auth stream")
            {
                Ok(AuthIncome::AuthProof(solution)) => break solution,
                Err(e) => log::error!("({log_state}) {e:?}"),
            };
        };

        match solution.verify(&challenge, &cert, &policy::StandardPolicy::new()) {
            Ok(_) => {
                self.auth_stream
                    .send(AuthOutgo::Verdict(true))
                    .await
                    .context("sending true verdict")?;

                Ok(self.invoker)
            }
            Err(e) => {
                self.auth_stream
                    .send(AuthOutgo::Verdict(false))
                    .await
                    .context("sedning false verdict")?;
                Err(e)
            }
        }
    }
}
