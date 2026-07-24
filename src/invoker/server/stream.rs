pub use toaster_lib_rs::server::stream::{
    auth::{InvokerToManager as AuthIncome, ManagerToInvoker as AuthOutgo, NAME as AUTH_NAME},
    judge::{InvokerToManager as JudgeIncome, ManagerToInvoker as JudgeOutgo, NAME as JUDGE_NAME},
    master::{
        InvokerToManager as MasterIncome, ManagerToInvoker as MasterOutgo, NAME as MASTER_NAME,
    },
};
