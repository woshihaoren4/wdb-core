use trait_async::trait_async;
use anyhow::Result;
use wd_event::Context;
use serde_json::Value;
use crate::config::Config;
use std::sync::Arc;

pub trait ToBytes {
    fn to_bytes(&self)->Vec<u8>;
}
pub trait Tags{
    fn tags(&self)->Vec<(String,i64)>;
}
pub trait CallBack{
    fn call_back(&mut self,_: Context);
}

pub trait Element: ToBytes +Tags+CallBack+Send+Sync{
}

impl<T> Element for T
    where T: ToBytes +Tags+CallBack+Send+Sync
{}

// type Element = dyn Serialization + From<Vec<u8>> + Tags + CallBack + Send + Sync;

#[trait_async]
pub trait Bucket<T>
where T:Element+From<Vec<u8>>
{
    async fn init(&self,_:Context)->Result<BucketMeta>;
    async fn close(&self,_:Context)->Result<()>;

    async fn add(&self,_: Context, elms:Vec<T>) ->Result<Vec<T>>;
    async fn delete(&self,_: Context, elms:Vec<T>) ->Result<Vec<T>>;
    async fn update(&self,_: Context, elms:Vec<T>) ->Result<Vec<T>>;
    async fn find(&self,_: Context, elms:Vec<Box<dyn Tags>>) ->Result<Vec<T>>;
}

#[trait_async]
pub trait BucketBuilder{
    async fn metadata(&self) ->BuilderMeta;
    async fn init(&self,_: Context,_: Config) -> Arc<dyn Bucket<Box<dyn Element>>>;
    async fn build(&self,_: Context,_:Value) -> Arc<dyn Bucket<Box<dyn Element>>>;
}

#[derive(Default,Hash,Clone)]
pub struct BucketMeta{
    pub name:String
}
#[derive(Default,Hash)]
pub struct BuilderMeta{
    pub bucket_type:String,
}