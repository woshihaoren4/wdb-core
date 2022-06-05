use super::*;
pub struct Entity{
    builders:Vec<Box<dyn BucketBuilder>>,
    buckets:Vec<Box<dyn Bucket<Box<dyn Element>>>>
}

impl Entity{
    pub fn new()->Entity{
        Entity{
            builders:vec![],
            buckets:vec![],
        }
    }
    pub fn register_builder<T:BucketBuilder>(mut self,builder:T)->Self{
        self.builders.push(Box::new(builder));self
    }
}