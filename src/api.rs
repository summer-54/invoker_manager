use toaster_lib_rs::judge::{submission, test};

pub struct SubmissionResult {
    pub result: submission::Result,
    pub tests: Box<[Option<test::Result>]>,
}
