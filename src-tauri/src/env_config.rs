use std::collections::HashMap;
use std::io::Read;

pub const S3_ENV_KEYS: &[&str] = &[
    "S3_BUCKET",
    "S3_REGION",
    "S3_ACCESS_KEY_ID",
    "S3_SECRET_ACCESS_KEY",
    "S3_ENDPOINT",
    "S3_FORCE_PATH_STYLE",
    "S3_SESSION_TOKEN",
    "S3_KEY_PREFIX",
    "S3_PUBLIC_BASE_URL",
    "S3_CONNECT_IPS",
    "S3_STALL_GRACE_SECONDS",
    "S3_PART_SIZE_MIB",
    "S3_UPLOAD_CONCURRENCY",
    "S3_SEND_CHECKSUM_HEADERS",
];

// Parse without modifying process-wide environment variables. Never include
// dotenv parser errors in user-facing messages: they can contain secret values.
pub fn parse_s3_env(reader: impl Read) -> Result<HashMap<String, String>, &'static str> {
    let mut values = HashMap::new();
    for entry in dotenvy::from_read_iter(reader) {
        let (key, value) = entry.map_err(|_| ".env 格式错误，请检查配置文件")?;
        if S3_ENV_KEYS.contains(&key.as_str()) {
            values.insert(key, value);
        }
    }
    Ok(values)
}
