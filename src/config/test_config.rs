use serde::{Serialize,Deserialize};

#[derive(Serialize,Deserialize,Clone)]
pub struct TestConfig{
    pub bucket_name:String,
    #[serde(default="default_path")]
    pub path:String,
}
fn default_path()->String{
    String::from(r#"E:\github.com\wdb-core\src\test"#)
}