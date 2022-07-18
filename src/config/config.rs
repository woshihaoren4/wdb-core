use serde::{Serialize,Deserialize};
use serde_json::Value;
use std::collections::HashMap;
use crate::util;

#[derive(Serialize,Deserialize,Clone)]
pub struct Config{
    pub server:Server,
    #[serde(default="default_bucket_engine")]
    pub bucket_engine:HashMap<String,Value>,
}
impl Default for Config{
    fn default() -> Self {
        Config{
            server:Server::default(),
            bucket_engine:default_bucket_engine(),
        }
    }
}

#[derive(Serialize,Deserialize,Clone)]
pub struct Server{
    #[serde(default="Server::default_worker_path")]
    pub worker_path:String,
    #[serde(default="Server::default_default_engine")]
    pub default_engine:String,
}
impl Server{
    fn default()->Server{
        Server{
            worker_path:Self::default_worker_path(),
            default_engine:Self::default_default_engine()}
    }
    fn default_worker_path()->String{
        let s = util::env_home_path_default(".");
        format!("{}/wdb",s)
    }
    fn default_default_engine()->String{"logdb".to_string()}
}
fn default_bucket_engine()->HashMap<String,Value>{
    HashMap::new()
}

#[cfg(test)]
mod config_test{
    use super::Config;
    #[test]
    fn  test_server(){
        let cfg = match super::super::load_config_form_file("./src/config/config.toml"){
            Ok(o)=>o,
            Err(e)=>panic!("{}",e.to_string())
        };
        let s = serde_json::to_string(&cfg).unwrap();
        println!("{}",s)
    }
}