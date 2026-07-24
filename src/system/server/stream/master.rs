use crate::prelude::*;
use toaster_lib_rs::{
    judge::{Lang, submission, test},
    server::RawMessage,
};
use uuid::Uuid;

pub const NAME: &str = "MASTER";

pub enum Income {
    Judge {
        id: Uuid,
        test_count: usize,
        lang: Lang,
        data: Box<[u8]>,
    },
}

impl super::Income for Income {
    fn from_raw(msg: toaster_lib_rs::server::MappedRawMessage) -> anyhow::Result<Self> {
        Ok(match msg.ty() {
            "JUDGE" => Self::Judge {
                id: msg
                    .field("ID")
                    .ok_or(anyhow!("{} filed not found", "ID".bold()))?
                    .parse()
                    .context("parsing 'ID' field")?,
                test_count: msg
                    .field("COUNT")
                    .ok_or(anyhow!("{} filed not found", "COUNT".bold()))?
                    .parse()
                    .context("parsing 'CODE' field")?,
                lang: msg
                    .field("LANG")
                    .ok_or(anyhow!("{} filed not found", "LANG".bold()))?
                    .try_into()
                    .context("parsing 'LANG' field")?,
                data: msg
                    .data()
                    .ok_or(anyhow!("{} not found", "data".bold()))?
                    .into(),
            },
            command => {
                bail!("incorrect command '{}'", command.bold())
            }
        })
    }
}

pub enum Outgo {
    FullResult {
        id: Uuid,
        verdict: submission::Result,
        tests: Box<[Option<test::Result>]>,
    },
    TestResult {
        id: Uuid,
        test_id: usize,
        verdict: test::Verdict,
        data: Box<[u8]>,
    },
}

impl super::Outgo for Outgo {
    fn into_raw(self) -> RawMessage {
        match self {
            Self::FullResult { id, verdict, tests } => {
                let mut body = RawMessage::new("VERDICT");
                body.add_field(&"SUBMISSION", &id);
                match verdict {
                    submission::Result::Ok {
                        score,
                        groups_score,
                    } => {
                        body.add_fields(vec![(&"VERDICT", &"OK"), (&"SUM", &score)]);
                        body.add_field(
                            &"GROUPS",
                            &groups_score
                                .into_iter()
                                .map(|score| format!("{score}"))
                                .collect::<Vec<_>>()
                                .join(" "),
                        );
                    }
                    submission::Result::Ce(msg) => {
                        body.add_field(&"VERDICT", &"CE");
                        body.add_field(&"MESSAGE", &msg);
                    }
                    submission::Result::Te(msg) => {
                        body.add_field(&"VERDICT", &"TE");
                        body.add_field(&"MESSAGE", &msg);
                    }
                }

                for (i, test) in tests.into_iter().enumerate() {
                    body.add_field(
                        &format!("T{}", i + 1),
                        &format!(
                            "{} {} {}",
                            test.as_ref()
                                .map(|test| test.verdict.to_string())
                                .unwrap_or("SK".to_string()),
                            test.as_ref().map(|test| test.time).unwrap_or(0.),
                            test.as_ref().map(|test| test.memory).unwrap_or(0),
                        ),
                    );
                }

                body
            }
            Outgo::TestResult {
                id,
                test_id,
                verdict,
                data,
            } => {
                let mut body = RawMessage::new("TEST");
                body.add_fields(vec![
                    (&"SUBMISSION", &id),
                    (&"TEST", &test_id),
                    (&"VERDICT", &verdict),
                ])
                .set_data(data);
                body
            }
        }
    }
}
