mod master;
pub use master::{Income as MasterIncome, NAME as MASTER_NAME, Outgo as MasterOutgo};
use toaster_lib_rs::server::stream::{Income, Outgo};
