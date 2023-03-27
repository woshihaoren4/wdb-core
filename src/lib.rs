mod common;
pub mod core;
mod local_index;
mod local_store;
mod db;

pub use core::*;
pub use db::*;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
