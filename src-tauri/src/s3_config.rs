use crate::env_config::{parse_s3_env, S3_ENV_KEYS};
use serde::Serialize;
use std::collections::HashMap;
use std::net::IpAddr;
use std::path::PathBuf;

const BUILT_S3_CONFIG: &str = include_str!(concat!(env!("OUT_DIR"), "/s3-config.json"));

pub struct S3Config {
    pub bucket: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
    pub endpoint: Option<String>,
    pub session_token: Option<String>,
    pub force_path_style: bool,
    pub key_prefix: String,
    pub public_base_url: Option<String>,
    pub connect_ips: Vec<IpAddr>,
    pub stall_grace_seconds: u64,
    pub part_size: u64,
    pub upload_concurrency: usize,
    pub send_checksum_headers: bool,
}

#[derive(Serialize)]
pub struct PublicS3Config {
    bucket: String,
    region: String,
    endpoint: Option<String>,
    key_prefix: String,
}

impl From<S3Config> for PublicS3Config {
    fn from(config: S3Config) -> Self {
        Self {
            bucket: config.bucket,
            region: config.region,
            endpoint: config.endpoint,
            key_prefix: config.key_prefix,
        }
    }
}

impl S3Config {
    pub fn load() -> Result<Self, String> {
        let mut values: HashMap<String, String> = serde_json::from_str(BUILT_S3_CONFIG)
            .map_err(|_| "无法读取构建时的 S3 配置".to_string())?;
        let env_path = runtime_env_path()?;
        match std::fs::File::open(&env_path) {
            Ok(file) => values.extend(parse_s3_env(file)?),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(format!("无法打开配置文件: {}", env_path.display())),
        }
        // Explicit process settings take precedence over file and build defaults.
        for key in S3_ENV_KEYS {
            match std::env::var(key) {
                Ok(value) => {
                    values.insert((*key).to_string(), value);
                }
                Err(std::env::VarError::NotPresent) => {}
                Err(_) => return Err(format!("{key} 必须是有效的 UTF-8 字符串")),
            }
        }
        Self::from_values(&values)
    }

    fn from_values(values: &HashMap<String, String>) -> Result<Self, String> {
        let required = |key: &str| {
            values
                .get(key)
                .filter(|value| !value.trim().is_empty())
                .cloned()
                .ok_or_else(|| format!("缺少 {key}，请在 .env 中配置后重新加载"))
        };
        let optional = |key: &str| {
            values
                .get(key)
                .filter(|value| !value.trim().is_empty())
                .cloned()
        };
        let force_path_style = match optional("S3_FORCE_PATH_STYLE").as_deref() {
            None | Some("false") => false,
            Some("true") => true,
            _ => return Err("S3_FORCE_PATH_STYLE 必须是 true 或 false".to_string()),
        };
        let send_checksum_headers = match optional("S3_SEND_CHECKSUM_HEADERS").as_deref() {
            None | Some("true") => true,
            Some("false") => false,
            _ => return Err("S3_SEND_CHECKSUM_HEADERS 必须是 true 或 false".into()),
        };
        let endpoint = optional("S3_ENDPOINT");
        let public_base_url = optional("S3_PUBLIC_BASE_URL");
        let connect_ips = optional("S3_CONNECT_IPS")
            .map(|value| {
                value
                    .split(',')
                    .map(|ip| ip.trim().parse::<IpAddr>())
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()
            .map_err(|_| "S3_CONNECT_IPS 必须是逗号分隔的 IP 地址")?
            .unwrap_or_default();
        if !connect_ips.is_empty() && endpoint.is_none() {
            return Err("配置 S3_CONNECT_IPS 时必须指定 S3_ENDPOINT".into());
        }
        let stall_grace_seconds = optional("S3_STALL_GRACE_SECONDS")
            .map(|value| value.parse::<u64>())
            .transpose()
            .map_err(|_| "S3_STALL_GRACE_SECONDS 必须是 20 到 300 的整数")?
            .unwrap_or(60);
        if !(20..=300).contains(&stall_grace_seconds) {
            return Err("S3_STALL_GRACE_SECONDS 必须是 20 到 300 的整数".into());
        }
        let part_mib = optional("S3_PART_SIZE_MIB")
            .map(|v| v.parse::<u64>())
            .transpose()
            .map_err(|_| "S3_PART_SIZE_MIB 必须是 5 到 64 的整数")?
            .unwrap_or(64);
        if !(5..=64).contains(&part_mib) {
            return Err("S3_PART_SIZE_MIB 必须是 5 到 64 的整数".into());
        }
        let upload_concurrency = optional("S3_UPLOAD_CONCURRENCY")
            .map(|v| v.parse::<usize>())
            .transpose()
            .map_err(|_| "S3_UPLOAD_CONCURRENCY 必须是 1 到 8 的整数")?
            .unwrap_or(3);
        if !(1..=8).contains(&upload_concurrency) {
            return Err("S3_UPLOAD_CONCURRENCY 必须是 1 到 8 的整数".into());
        }
        for (key, value) in [
            ("S3_ENDPOINT", &endpoint),
            ("S3_PUBLIC_BASE_URL", &public_base_url),
        ] {
            if let Some(value) = value {
                let parsed = url::Url::parse(value)
                    .map_err(|_| format!("{key} 必须是有效的 HTTP(S) 地址"))?;
                if !matches!(parsed.scheme(), "http" | "https")
                    || parsed.host_str().is_none()
                    || !parsed.username().is_empty()
                    || parsed.password().is_some()
                    || parsed.query().is_some()
                    || parsed.fragment().is_some()
                {
                    return Err(format!("{key} 必须是无凭证、查询参数及片段的 HTTP(S) 地址"));
                }
            }
        }
        Ok(Self {
            bucket: required("S3_BUCKET")?,
            region: optional("S3_REGION").unwrap_or_else(|| "us-east-1".to_string()),
            access_key: required("S3_ACCESS_KEY_ID")?,
            secret_key: required("S3_SECRET_ACCESS_KEY")?,
            endpoint,
            public_base_url,
            connect_ips,
            stall_grace_seconds,
            part_size: part_mib * 1024 * 1024,
            upload_concurrency,
            send_checksum_headers,
            session_token: optional("S3_SESSION_TOKEN"),
            force_path_style,
            key_prefix: optional("S3_KEY_PREFIX")
                .unwrap_or_default()
                .trim_matches('/')
                .to_string(),
        })
    }
}

fn runtime_env_path() -> Result<PathBuf, String> {
    if cfg!(debug_assertions) {
        return Ok(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../.env"));
    }
    // AppImages extract the executable into a temporary mount. Use the original
    // image location so its sibling .env remains editable after distribution.
    let executable = std::env::var_os("APPIMAGE")
        .map(PathBuf::from)
        .map(Ok)
        .unwrap_or_else(std::env::current_exe)
        .map_err(|_| "无法确定应用程序目录".to_string())?;
    executable
        .parent()
        .map(|path| path.join(".env"))
        .ok_or_else(|| "无法确定应用程序目录".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn values() -> HashMap<String, String> {
        parse_s3_env(&b"S3_BUCKET=firmware\nS3_ACCESS_KEY_ID=test-key\nS3_SECRET_ACCESS_KEY=test-secret\n"[..]).unwrap()
    }

    #[test]
    fn defaults_and_public_config_do_not_expose_credentials() {
        let config = S3Config::from_values(&values()).unwrap();
        assert_eq!(config.region, "us-east-1");
        assert!(!config.force_path_style);
        let public = serde_json::to_value(PublicS3Config::from(config)).unwrap();
        assert_eq!(public["bucket"], "firmware");
        assert_eq!(public.as_object().unwrap().len(), 4);
        assert!(!public.to_string().contains("test-key"));
        assert!(!public.to_string().contains("test-secret"));
    }

    #[test]
    fn runtime_file_overrides_build_values_and_supports_quoted_secrets() {
        let mut merged = values();
        merged.extend(parse_s3_env(&b"S3_BUCKET=other\nS3_SECRET_ACCESS_KEY='secret # with spaces'\nS3_ENDPOINT=https://storage.example.com\nS3_FORCE_PATH_STYLE=true\nS3_SESSION_TOKEN=token\nS3_KEY_PREFIX=/releases/\nS3_PUBLIC_BASE_URL=https://cdn.example.com\nUNRELATED_SECRET=ignored\n"[..]).unwrap());
        let config = S3Config::from_values(&merged).unwrap();
        assert_eq!(config.bucket, "other");
        assert_eq!(config.secret_key, "secret # with spaces");
        assert_eq!(
            config.endpoint.as_deref(),
            Some("https://storage.example.com")
        );
        assert_eq!(config.session_token.as_deref(), Some("token"));
        assert!(config.force_path_style);
        assert_eq!(config.key_prefix, "releases");
        assert_eq!(
            config.public_base_url.as_deref(),
            Some("https://cdn.example.com")
        );
        assert!(!merged.contains_key("UNRELATED_SECRET"));
    }

    #[test]
    fn invalid_config_reports_setting_names_without_secret_values() {
        let mut missing = values();
        missing.insert("S3_SECRET_ACCESS_KEY".into(), "   ".into());
        assert!(S3Config::from_values(&missing)
            .err()
            .unwrap()
            .contains("S3_SECRET_ACCESS_KEY"));
        let mut invalid = values();
        invalid.insert("S3_FORCE_PATH_STYLE".into(), "secret-value".into());
        assert_eq!(
            S3Config::from_values(&invalid).err().unwrap(),
            "S3_FORCE_PATH_STYLE 必须是 true 或 false"
        );
        assert_eq!(
            parse_s3_env(&b"S3_SECRET_ACCESS_KEY='secret-value"[..]).unwrap_err(),
            ".env 格式错误，请检查配置文件"
        );
        invalid.insert("S3_FORCE_PATH_STYLE".into(), "false".into());
        invalid.insert("S3_ENDPOINT".into(), "invalid-url".into());
        assert!(S3Config::from_values(&invalid).is_err());
        invalid.remove("S3_ENDPOINT");
        invalid.insert(
            "S3_PUBLIC_BASE_URL".into(),
            "https://secret-user:secret-password@cdn.example.com".into(),
        );
        let error = S3Config::from_values(&invalid).err().unwrap();
        assert!(error.contains("S3_PUBLIC_BASE_URL"));
        assert!(!error.contains("secret-user"));
        assert!(!error.contains("secret-password"));
    }

    #[test]
    fn validates_client_scoped_routing_and_stall_protection() {
        let mut input = values();
        input.insert("S3_CONNECT_IPS".into(), "203.0.113.10,2001:db8::1".into());
        assert!(S3Config::from_values(&input).is_err());
        input.insert("S3_ENDPOINT".into(), "https://storage.example.com".into());
        input.insert("S3_STALL_GRACE_SECONDS".into(), "90".into());
        let config = S3Config::from_values(&input).unwrap();
        assert_eq!(config.connect_ips.len(), 2);
        assert_eq!(config.stall_grace_seconds, 90);
        input.insert("S3_PART_SIZE_MIB".into(), "8".into());
        input.insert("S3_UPLOAD_CONCURRENCY".into(), "1".into());
        let tuned = S3Config::from_values(&input).unwrap();
        assert_eq!(tuned.part_size, 8 * 1024 * 1024);
        assert_eq!(tuned.upload_concurrency, 1);
        for (key, bad) in [
            ("S3_PART_SIZE_MIB", "4"),
            ("S3_PART_SIZE_MIB", "65"),
            ("S3_UPLOAD_CONCURRENCY", "0"),
            ("S3_UPLOAD_CONCURRENCY", "9"),
        ] {
            let mut invalid = input.clone();
            invalid.insert(key.into(), bad.into());
            assert!(S3Config::from_values(&invalid).is_err());
        }
        input.insert("S3_CONNECT_IPS".into(), "not-an-ip".into());
        assert!(S3Config::from_values(&input).is_err());
        input.remove("S3_CONNECT_IPS");
        for invalid in ["0", "19", "301", "secret-input"] {
            input.insert("S3_STALL_GRACE_SECONDS".into(), invalid.into());
            assert_eq!(
                S3Config::from_values(&input).err().unwrap(),
                "S3_STALL_GRACE_SECONDS 必须是 20 到 300 的整数"
            );
        }
    }
}
