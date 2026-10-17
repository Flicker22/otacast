//! Connection routing is scoped to this S3 client. URLs, signing and TLS SNI
//! keep their original host names; no system resolver or proxy is modified.
use crate::s3_config::S3Config;
use aws_smithy_http_client::{tls, Builder};
use aws_smithy_runtime_api::client::dns::{DnsFuture, ResolveDns, ResolveDnsError};
use std::net::IpAddr;

#[derive(Clone, Debug)]
struct EndpointResolver {
    hosts: Vec<String>,
    addresses: Vec<IpAddr>,
}

impl ResolveDns for EndpointResolver {
    fn resolve_dns<'a>(&'a self, name: &'a str) -> DnsFuture<'a> {
        DnsFuture::new(async move {
            if self
                .hosts
                .iter()
                .any(|host| host.eq_ignore_ascii_case(name.trim_end_matches('.')))
            {
                return Ok(self.addresses.clone());
            }
            let mut addresses: Vec<_> = tokio::net::lookup_host((name, 0))
                .await
                .map_err(ResolveDnsError::new)?
                .map(|address| address.ip())
                .collect();
            addresses.sort_unstable();
            addresses.dedup();
            Ok(addresses)
        })
    }
}

pub fn http_client(settings: &S3Config) -> Option<aws_sdk_s3::config::SharedHttpClient> {
    if settings.connect_ips.is_empty() {
        return None;
    }
    // S3Config validates the endpoint before constructing this transport.
    let endpoint = url::Url::parse(settings.endpoint.as_deref()?).ok()?;
    let host = endpoint.host_str()?.to_string();
    let hosts = vec![format!("{}.{}", settings.bucket, host), host];
    Some(
        Builder::new()
            .tls_provider(tls::Provider::Rustls(
                tls::rustls_provider::CryptoMode::AwsLc,
            ))
            .build_with_resolver(EndpointResolver {
                hosts,
                addresses: settings.connect_ips.clone(),
            }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn overrides_only_storage_hosts_and_preserves_system_resolution_elsewhere() {
        let resolver = EndpointResolver {
            hosts: vec![
                "bucket.storage.example.com".into(),
                "storage.example.com".into(),
            ],
            addresses: vec!["203.0.113.10".parse().unwrap()],
        };
        assert_eq!(
            resolver
                .resolve_dns("BUCKET.storage.example.com.")
                .await
                .unwrap(),
            resolver.addresses
        );
        assert_eq!(
            resolver.resolve_dns("storage.example.com").await.unwrap(),
            resolver.addresses
        );
        let local = resolver.resolve_dns("localhost").await.unwrap();
        assert!(!local.is_empty());
        assert!(local.iter().all(|ip| ip.is_loopback()));
    }
}
