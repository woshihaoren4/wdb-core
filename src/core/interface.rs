use trait_async::trait_async;
use anyhow::Result;
use async_channel::{Receiver,Sender};
use std::collections::HashMap;
use wd_event::Context;
use serde_json::Value;

pub trait Serialization{
    fn to_bytes(&self)->Vec<u8>;
}
pub trait Tags{
    fn tags(&self)->Vec<(String,i64)>;
}
pub trait CallBack{
    fn call_back(&mut self,_: Context);
}

pub trait Element:Serialization+Tags+CallBack+Send+Sync{
}

impl<T> Element for T
    where T:Serialization+Tags+CallBack+Send+Sync
{}

// type Element = dyn Serialization + From<Vec<u8>> + Tags + CallBack + Send + Sync;

#[trait_async]
pub trait Bucket<T>
where T:Element+From<Vec<u8>>
{
    async fn add(&self,_: Context, elms:Vec<T>) ->Result<Vec<T>>;
    async fn delete(&self,_: Context, elms:Vec<T>) ->Result<Vec<T>>;
    async fn update(&self,_: Context, elms:Vec<T>) ->Result<Vec<T>>;
    async fn find(&self,_: Context, elms:Vec<u64>) ->Result<Vec<T>>;
}

#[trait_async]
pub trait BucketBuilder{
    async fn build(&self,_:Value) -> Box<dyn Bucket<Box<dyn Element>>>;
}

// //存储索引的接口
// #[trait_async]
// pub trait Index {
//      async fn insert(&mut self,tag:IndexValue)->Result<()>;
//      async fn delete(&mut self,tag:IndexValue)->Result<()>;
//      async fn find(&self,eq:IndexValue,mix:IndexValue,max:IndexValue,sender:Sender<Result<u64>>);
// }