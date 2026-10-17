use crate::s3_config::S3Config;
use url::Url;

pub fn object_url(settings: &S3Config, key: &str) -> Result<String, String> {
    if key.trim().is_empty() || key.split('/').any(|segment| matches!(segment, "." | "..")) {
        return Err("上传路径不能为空，也不能包含独立的 . 或 .. 路径段".to_string());
    }
    let mut base = if let Some(base) = &settings.public_base_url {
        Url::parse(base).map_err(|_| "文件访问域名无效")?
    } else if let Some(endpoint) = &settings.endpoint {
        let mut url = Url::parse(endpoint).map_err(|_| "S3 Endpoint 无效")?;
        let ip_host = matches!(url.host(), Some(url::Host::Ipv4(_) | url::Host::Ipv6(_)));
        if settings.force_path_style
            || ip_host
            || url.host_str() == Some("localhost")
            || (settings.bucket.contains('.') && url.scheme() == "https")
        {
            url.path_segments_mut()
                .map_err(|_| "S3 Endpoint 无效")?
                .pop_if_empty()
                .push(&settings.bucket);
        } else {
            let host = format!(
                "{}.{}",
                settings.bucket,
                url.host_str().ok_or("S3 Endpoint 无效")?
            );
            url.set_host(Some(&host)).map_err(|_| "S3 Endpoint 无效")?;
        }
        url
    } else {
        let suffix = if settings.region.starts_with("cn-") {
            "amazonaws.com.cn"
        } else {
            "amazonaws.com"
        };
        let path_style = settings.force_path_style || settings.bucket.contains('.');
        let host = if path_style {
            format!("https://s3.{}.{suffix}", settings.region)
        } else {
            format!(
                "https://{}.s3.{}.{suffix}",
                settings.bucket, settings.region
            )
        };
        let mut url = Url::parse(&host).map_err(|_| "Bucket 或 Region 无效")?;
        if path_style {
            url.path_segments_mut()
                .map_err(|_| "文件访问地址无效")?
                .push(&settings.bucket);
        }
        url
    };
    base.path_segments_mut()
        .map_err(|_| "文件访问地址无效")?
        .pop_if_empty()
        .extend(key.split('/'));
    Ok(base.to_string())
}

pub fn md5_url(file_url: &str, md5: &str) -> Result<String, String> {
    let mut url = Url::parse(file_url).map_err(|_| "文件 URL 无效")?;
    url.query_pairs_mut().append_pair("md5", md5);
    Ok(url.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings() -> S3Config {
        S3Config {
            bucket: "firmware".into(),
            region: "us-east-1".into(),
            access_key: "test".into(),
            secret_key: "test".into(),
            endpoint: None,
            session_token: None,
            force_path_style: false,
            key_prefix: String::new(),
            public_base_url: None,
            connect_ips: Vec::new(),
            stall_grace_seconds: 60,
            part_size: crate::upload_store::PART_SIZE,
            upload_concurrency: crate::upload_store::CONCURRENCY,
            send_checksum_headers: true,
        }
    }

    #[test]
    fn builds_aws_endpoint_and_public_urls_with_encoded_keys() {
        let mut config = settings();
        assert_eq!(
            object_url(&config, "releases/a b#%.bin").unwrap(),
            "https://firmware.s3.us-east-1.amazonaws.com/releases/a%20b%23%25.bin"
        );
        config.endpoint = Some("https://storage.example.com/root".into());
        assert_eq!(
            object_url(&config, "a.bin").unwrap(),
            "https://firmware.storage.example.com/root/a.bin"
        );
        config.force_path_style = true;
        assert_eq!(
            object_url(&config, "a.bin").unwrap(),
            "https://storage.example.com/root/firmware/a.bin"
        );
        config.public_base_url = Some("https://cdn.example.com/files/".into());
        assert_eq!(
            object_url(&config, "固件.bin").unwrap(),
            "https://cdn.example.com/files/%E5%9B%BA%E4%BB%B6.bin"
        );
        assert!(object_url(&config, "../a.bin").is_err());
    }

    #[test]
    fn supports_local_endpoints_china_and_existing_query_parameters() {
        let mut config = settings();
        config.endpoint = Some("http://127.0.0.1:9000".into());
        assert_eq!(
            object_url(&config, "a.bin").unwrap(),
            "http://127.0.0.1:9000/firmware/a.bin"
        );
        config.endpoint = None;
        config.region = "cn-north-1".into();
        assert_eq!(
            object_url(&config, "a.bin").unwrap(),
            "https://firmware.s3.cn-north-1.amazonaws.com.cn/a.bin"
        );
        assert_eq!(
            md5_url("https://cdn.example.com/a.bin?download=1#section", "abc").unwrap(),
            "https://cdn.example.com/a.bin?download=1&md5=abc#section"
        );
    }
}
