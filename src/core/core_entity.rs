use super::*;
use std::sync::Arc;
use crate::config::Config;

pub struct Entity{
    config:Config,
    builders:Vec<Box<dyn BucketBuilder>>,
    buckets:Vec<(BucketMeta,Arc<dyn Bucket<Box<dyn Element>>>)>
}

impl Entity{
    pub fn new(config:Config)->Entity{
        Entity{
            config,
            builders:vec![],
            buckets:vec![],
        }
    }
    pub fn register_builder<T:BucketBuilder+'static>(mut self,builder:T)->Self{
        self.builders.push(Box::new(builder));self
    }
}