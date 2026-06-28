macro_rules! build_env {
    ($name:ident) => {
        env!(concat!("MODELPOINTER_", stringify!($name)))
    };
}

pub const PROJECT_NAME: &str = build_env!(PROJECT_NAME);
pub const VERSION: &str = build_env!(VERSION);

/// Get simple version string (default for --version)
pub fn get_version_string() -> String {
    format!("{} {}", PROJECT_NAME, VERSION)
}