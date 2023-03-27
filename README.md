# wdb-core
wd database core module


## fast start
```rust
let bucket = WDBuilder::new()
    .dir("./database".into())
    .name("test".into())
    .block_size(1024 * 1024 * 32)
    .map(|x| {
        wd_log::log_debug_ln!("path--->{}/{}",x.dir,x.name);
        x
    })
    .build()
    .await
    .expect("build wdb error:");

bucket.set(1,b"hello world").await.expect("insert failed");
let value = bucket.find(&1).await.expect("find error");
assert_eq!(b"hello world",value.as_slice(),"test one");

bucket.set(2,b"wdb every nb").await.expect("insert failed");
let value = bucket.find(&2).await.expect("find error");
assert_eq!(b"wdb every nb",value.as_slice(),"test one");

tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
```