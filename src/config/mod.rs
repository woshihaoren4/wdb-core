mod test_config;
mod config;

#[cfg(test)]
pub use test_config::TestConfig;

pub use config::Config;
use std::path::Path;

pub fn load_config_form_file(path:impl AsRef<Path>)->anyhow::Result<Config>{
    match wd_run::load_config(path) {
        Ok(o)=>Ok(o),
        Err(e)=>Err(anyhow::anyhow!("load config form file error:{}",e))
    }
}