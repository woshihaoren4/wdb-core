#![allow(dead_code)]

use std::env::var_os;
use std::ffi::OsStr;

pub fn get_env<T:AsRef<OsStr>>(key:T)->Option<String> {
    if let Some(s) = var_os(key) {
        return Some(s.to_str().unwrap_or("").to_string())
    }
    None
}

pub fn env_home_path_default<S:ToString>(default:S)->String{
    if let Some(s) = get_env("HOME"){
        return s
    }else if let Some(s) = get_env("USERPROFILE"){
        return s
    }
    return default.to_string()
}

pub fn env_home_path_func<F>(function:F)->String
    where F: Fn()->String
{
    if let Some(s) = get_env("HOME"){
        return s
    }else if let Some(s) = get_env("USERPROFILE"){
        return s
    }
    return function()
}

#[cfg(test)]
mod env_test{
    // use super::home_path_default;
    use super::{env_home_path_default, env_home_path_func};

    #[test]
    fn get_env(){
        let path = env_home_path_default("aaa");
        assert_eq!(path.as_str(),r"C:\Users\Administrator")
    }

    #[test]
    fn test_func(){
        let path = env_home_path_func(||{
            #[cfg(target_os  =  "linux")]
            let path = String::from("hello world");
            #[cfg(target_os = "windows")]
            let path = String::from("haha");
            return path
        });
        assert_eq!(path.as_str(),"haha")
    }
}