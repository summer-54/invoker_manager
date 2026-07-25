use crate::prelude::*;
use toaster_lib_rs::{
    judge::{Lang, submission, test},
    logger::short_slice,
    server::RawMessage,
};
use uuid::Uuid;

pub const NAME: &str = "master";

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

impl std::fmt::Debug for Outgo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FullResult { id, verdict, tests } => f
                .debug_struct("FullResult")
                .field("id", id)
                .field("verdict", verdict)
                .field("tests", tests)
                .finish(),
            Self::TestResult {
                id,
                test_id,
                verdict,
                data,
            } => f
                .debug_struct("TestVerdict")
                .field("id", id)
                .field("test_id", test_id)
                .field("verdict", verdict)
                .field("data", &Box::<[u8]>::from(short_slice(data)))
                .finish(),
        }
    }
}

impl super::Outgo for Outgo {
    fn into_raw(self) -> RawMessage {
        match self {
            Self::FullResult { id, verdict, tests } => {
                let mut body = RawMessage::new("VERDICT");
                body.add_field(&"ID", &id);
                match verdict {
                    submission::Result::Ok {
                        score,
                        groups_score,
                    } => {
                        body.add_fields(vec![(&"NAME", &"OK"), (&"SUM", &score)]);
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
                        body.add_field(&"NAME", &"CE");
                        body.add_field(&"MESSAGE", &msg);
                    }
                    submission::Result::Te(msg) => {
                        body.add_field(&"NAME", &"TE");
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
                    (&"ID", &id),
                    (&"TEST", &test_id),
                    (&"VERDICT", &verdict),
                ])
                .set_data(data);
                body
            }
        }
    }
}
