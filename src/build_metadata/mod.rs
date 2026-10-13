// Include generated available features
// This provides: pub const WORKSPACE_FEATURES: &[(&str, &[&str])]
include!(concat!(env!("OUT_DIR"), "/available_features.rs"));

pub static RUSTC_VERSION: Option<&str> = option_env!("RUSTC_VERSION");
pub static HOST_OS: Option<&str> = option_env!("HOST_OS");
pub static HOST_ARCH: Option<&str> = option_env!("HOST_ARCH");

pub static PROFILE: Option<&str> = option_env!("PROFILE");
pub static OPT_LEVEL: Option<&str> = option_env!("OPT_LEVEL");
pub static DEBUG: Option<&str> = option_env!("DEBUG");
pub static TARGET: Option<&str> = option_env!("TARGET");
pub static HOST: Option<&str> = option_env!("HOST");

pub static CFG_ENDIAN: Option<&str> = option_env!("CFG_ENDIAN");
pub static CFG_POINTER_WIDTH: Option<&str> = option_env!("CFG_POINTER_WIDTH");
pub static CFG_ENV: Option<&str> = option_env!("CFG_ENV");
