//! Opt-in acceptance tests against the configured object store.
use super::*;
use aws_sdk_s3::config::{
    interceptors::{BeforeTransmitInterceptorContextRef, FinalizerInterceptorContextRef},
    ConfigBag, Intercept, RuntimeComponents,
};
use std::error::Error;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Logs only protocol fields needed for diagnosis. Never dumps signed requests,
/// Authorization, session tokens, cookies, or complete SDK Debug structures.
struct LiveDiagnostics {
    secrets: Vec<String>,
}

impl std::fmt::Debug for LiveDiagnostics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("LiveDiagnostics { secrets: [redacted] }")
    }
}

impl LiveDiagnostics {
    fn new(settings: &S3Config) -> Self {
        Self {
            secrets: [
                Some(settings.access_key.clone()),
                Some(settings.secret_key.clone()),
                settings.session_token.clone(),
            ]
            .into_iter()
            .flatten()
            .filter(|s| !s.is_empty())
            .collect(),
        }
    }

    fn redact(&self, value: String) -> String {
        self.secrets
            .iter()
            .fold(value, |value, secret| value.replace(secret, "[redacted]"))
    }
}

impl Intercept for LiveDiagnostics {
    fn name(&self) -> &'static str {
        "OTACastLiveDiagnostics"
    }

    fn read_before_transmit(
        &self,
        context: &BeforeTransmitInterceptorContextRef<'_>,
        _: &RuntimeComponents,
        _: &mut ConfigBag,
    ) -> std::result::Result<(), Box<dyn Error + Send + Sync>> {
        let request = context.request();
        let uri = url::Url::parse(request.uri()).unwrap();
        let part = uri
            .query_pairs()
            .find(|(k, _)| k == "partNumber")
            .map(|(_, v)| v.into_owned());
        let fields: Vec<_> = [
            "content-length",
            "content-encoding",
            "transfer-encoding",
            "expect",
            "amz-sdk-request",
            "x-amz-checksum-algorithm",
            "x-amz-sdk-checksum-algorithm",
            "x-amz-checksum-mode",
        ]
        .into_iter()
        .filter_map(|name| {
            request
                .headers()
                .get(name)
                .map(|value| format!("{name}={value}"))
        })
        .collect();
        println!(
            "live_request method={} part={part:?} {}",
            request.method(),
            fields.join(" ")
        );
        Ok(())
    }

    fn read_after_attempt(
        &self,
        context: &FinalizerInterceptorContextRef<'_>,
        _: &RuntimeComponents,
        _: &mut ConfigBag,
    ) -> std::result::Result<(), Box<dyn Error + Send + Sync>> {
        if let Some(response) = context.response() {
            let fields: Vec<_> = [
                "x-amz-checksum-sha256",
                "x-amz-checksum-type",
                "x-amz-request-id",
                "x-obs-request-id",
                "server",
                "content-length",
            ]
            .into_iter()
            .filter_map(|name| {
                response
                    .headers()
                    .get(name)
                    .map(|value| format!("{name}={value}"))
            })
            .collect();
            println!(
                "live_response status={} {}",
                response.status().as_u16(),
                fields.join(" ")
            );
        }
        if let Some(Err(error)) = context.output_or_error() {
            let mut chain = vec![error.to_string()];
            let mut cause = error.source();
            while let Some(source) = cause {
                chain.push(source.to_string());
                cause = source.source();
                if chain.len() >= 12 {
                    break;
                }
            }
            println!("live_error_chain={}", self.redact(chain.join(" -> ")));
        }
        Ok(())
    }
}

fn error_text<E: ProvideErrorMetadata + std::fmt::Debug>(
    context: &str,
    error: aws_sdk_s3::error::SdkError<E>,
) -> String {
    match aws_error(context, error) {
        Failure::Error(message) => message,
        _ => unreachable!(),
    }
}

async fn head_owned(
    client: &Client,
    task: &Task,
) -> std::result::Result<Option<aws_sdk_s3::operation::head_object::HeadObjectOutput>, String> {
    match client
        .head_object()
        .bucket(&task.target.bucket)
        .key(&task.key)
        .send()
        .await
    {
        Ok(head) => {
            if head.metadata().and_then(|m| m.get("otacast-task")) != Some(&task.id) {
                return Err("测试对象任务标识不匹配，停止下载和删除".into());
            }
            Ok(Some(head))
        }
        Err(error)
            if error
                .raw_response()
                .is_some_and(|r| r.status().as_u16() == 404) =>
        {
            Ok(None)
        }
        Err(error) => Err(error_text("查询测试对象失败", error)),
    }
}

async fn download_and_verify(
    client: &Client,
    task: &Task,
    expected_md5: &str,
) -> std::result::Result<(), String> {
    let head = head_owned(client, task).await?.ok_or("测试对象不存在")?;
    let mut response = client
        .get_object()
        .bucket(&task.target.bucket)
        .key(&task.key)
        .set_version_id(head.version_id().map(str::to_string))
        .set_if_match(head.e_tag().map(str::to_string))
        .send()
        .await
        .map_err(|e| error_text("下载测试对象失败", e))?;
    if response.metadata().and_then(|m| m.get("otacast-task")) != Some(&task.id) {
        return Err("下载的对象不属于本次测试".into());
    }
    let mut received = 0u64;
    let mut md5 = crate::Md5::new();
    while let Some(chunk) = response.body.next().await {
        let chunk = chunk.map_err(|e| format!("下载数据失败: {e}"))?;
        received += chunk.len() as u64;
        md5.update(&chunk);
    }
    if received != task.file_info.file_size {
        return Err(format!("下载大小不一致: {received}"));
    }
    if hex::encode(md5.finalize()) != expected_md5 {
        return Err("下载内容 MD5 与源文件不一致".into());
    }
    Ok(())
}

async fn cleanup_live(
    client: &Client,
    task: &Task,
    store: &Store,
) -> std::result::Result<(), String> {
    let mut errors = Vec::new();
    if let Ok(saved) = store.load(&task.id) {
        if let Err(error) = cleanup(client, &saved, store).await {
            errors.push(error);
        }
    }
    // This test owns a unique key. Check its metadata before removing precisely
    // the version created by the test; never enumerate or delete other objects.
    match head_owned(client, task).await {
        Ok(Some(head)) => {
            if let Err(error) = client
                .delete_object()
                .bucket(&task.target.bucket)
                .key(&task.key)
                .set_version_id(head.version_id().map(str::to_string))
                .send()
                .await
            {
                errors.push(error_text("删除测试对象失败", error));
            } else {
                match head_owned(client, task).await {
                    Ok(None) => {}
                    Ok(Some(_)) => errors.push("删除后测试对象仍存在".into()),
                    Err(error) => errors.push(error),
                }
            }
        }
        Ok(None) => {}
        Err(error) => errors.push(error),
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

async fn live_transfer(size: u64, checksum: bool) {
    let settings = S3Config::load().expect("无法读取真实存储配置");
    let mut config = client(&settings).config().to_builder();
    if std::env::var("OTACAST_LIVE_DIAGNOSTICS").as_deref() == Ok("1") {
        config = config.interceptor(LiveDiagnostics::new(&settings));
    }
    if let Ok(seconds) = std::env::var("OTACAST_LIVE_STALL_GRACE_SECONDS") {
        let seconds = seconds.parse::<u64>().expect("停滞保护秒数无效");
        assert!(seconds > 0);
        config = config.stalled_stream_protection(
            aws_sdk_s3::config::StalledStreamProtectionConfig::enabled()
                .grace_period(Duration::from_secs(seconds))
                .build(),
        );
        println!("live_test_override stall_grace_seconds={seconds}");
    }
    if let Ok(attempts) = std::env::var("OTACAST_LIVE_MAX_ATTEMPTS") {
        let attempts = attempts.parse::<u32>().expect("尝试次数无效");
        assert!(attempts > 0);
        config = config.retry_config(RetryConfig::standard().with_max_attempts(attempts));
        println!("live_test_override max_attempts={attempts}");
    }
    let client = Client::from_conf(config.build());
    let dir = tempfile::tempdir().unwrap();
    let (mut task, expected_md5) = super::sizes::sized_task(&dir, size).await;
    let unique = format!(
        "{:032x}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    task.id = format!(
        "{}-{}-{}-{}-{}",
        &unique[..8],
        &unique[8..12],
        &unique[12..16],
        &unique[16..20],
        &unique[20..]
    );
    task.target = Target::from(&settings);
    task.key = format!(
        "{}/_otacast-tests/{}/{size}.bin",
        settings.key_prefix, task.id
    )
    .trim_start_matches('/')
    .to_string();
    task.file_url = object_url::object_url(&settings, &task.key).unwrap();
    task.checksum = checksum;
    task.remote_checksums = settings.send_checksum_headers;
    task.part_size = settings.part_size;
    task.concurrency = settings.upload_concurrency;
    let store = store_at(&dir);
    store.save(&task).unwrap();
    let manager = UploadManager::default();
    let lease = manager.acquire(&task.id).unwrap();
    println!(
        "live_start size={size} checksum={checksum} key={}",
        task.key
    );
    let started = Instant::now();
    let last_update = Arc::new(Mutex::new(Instant::now()));
    let outcome = run(
        client.clone(),
        task.clone(),
        store.clone(),
        Arc::new(move |progress| {
            let mut last = last_update.lock().unwrap();
            if last.elapsed() >= Duration::from_secs(20) || progress.phase == "complete" {
                println!(
                    "live_progress phase={} bytes={}/{}",
                    progress.phase, progress.uploaded_bytes, progress.total_bytes
                );
                *last = Instant::now();
            }
        }),
        &lease,
    )
    .await;
    let upload_seconds = started.elapsed().as_secs_f64();
    if outcome.is_err() && task.checksum {
        // Preserve the application's failure, but distinguish a missing remote
        // checksum from corrupt content by inspecting HEAD and downloading.
        match client
            .head_object()
            .bucket(&task.target.bucket)
            .key(&task.key)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await
        {
            Ok(head) => println!(
                "live_checksum_diagnostic sha256_present={} sha256={:?} type={:?}",
                head.checksum_sha256().is_some(),
                head.checksum_sha256(),
                head.checksum_type()
            ),
            Err(error) => println!(
                "live_checksum_diagnostic={}",
                error_text("HEAD failed", error)
            ),
        }
        println!(
            "live_failed_upload_download_check={:?}",
            download_and_verify(&client, &task, &expected_md5).await
        );
    }
    let checked = async {
        let outcome = outcome?;
        let result = outcome.result.ok_or("上传未返回结果")?;
        if outcome.status != "complete" {
            return Err("上传未完成".to_string());
        }
        let download_started = Instant::now();
        download_and_verify(&client, &task, &expected_md5).await?;
        let md5 = crate::calculate_md5_impl(&task.file_path, size, &task.file_info.modified_at, &result.file_url)
            .map_err(|e| e.to_string())?;
        if md5.md5_hash != expected_md5 {
            return Err("本地按需 MD5 不一致".to_string());
        }
        println!("live_pass size={size} parts={} upload={upload_seconds:.2}s download_and_md5={:.2}s verification={} download_md5={expected_md5}", task.part_count(), download_started.elapsed().as_secs_f64(), result.verification);
        Ok(())
    }
    .await;
    let cleaned = cleanup_live(&client, &task, &store).await;
    if let Err(error) = cleaned {
        // A successful run has already removed its upload journal. Recreate a
        // credential-free record so a cleanup failure still has a target/path.
        let journal_error = if store.load(&task.id).is_err() {
            store.save(&task).err()
        } else {
            None
        };
        let path = dir.keep();
        panic!(
            "测试对象清理失败: {error}; key={}; journal={}; journal_error={journal_error:?}; upload_result={checked:?}",
            task.key,
            path.display()
        );
    }
    println!("live_cleanup=confirmed key={}", task.key);
    checked.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "uploads/downloads/deletes unique test objects in the real configured bucket; explicit opt-in required"]
async fn configured_s3_upload_download_and_cleanup() {
    assert_eq!(
        std::env::var("OTACAST_RUN_LIVE_S3").as_deref(),
        Ok("1"),
        "真实存储测试须显式设置 OTACAST_RUN_LIVE_S3=1"
    );
    if let Ok(path) = std::env::var("OTACAST_LIVE_CLEANUP_TASK") {
        let path = std::path::Path::new(&path);
        let store = Store {
            root: path.parent().unwrap().to_path_buf(),
        };
        let id = path.file_stem().unwrap().to_str().unwrap();
        let task = store.load(id).unwrap();
        let settings = S3Config::load().unwrap();
        assert_eq!(task.target, Target::from(&settings));
        assert!(task.key.contains(&format!("_otacast-tests/{}/", task.id)));
        cleanup_live(&client(&settings), &task, &store)
            .await
            .unwrap();
        println!("live_recovery_cleanup=confirmed key={}", task.key);
        return;
    }
    let checksum = match std::env::var("OTACAST_LIVE_CHECKSUM").as_deref() {
        Ok("false") => false,
        Ok("true") | Err(_) => true,
        _ => panic!("OTACAST_LIVE_CHECKSUM 仅接受 true 或 false"),
    };
    let sizes =
        std::env::var("OTACAST_LIVE_SIZES_BYTES").unwrap_or_else(|_| "1048576,135266304".into());
    for size in sizes.split(',') {
        live_transfer(size.trim().parse().expect("文件大小必须是字节数"), checksum).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real S3 transport control using curl; explicit opt-in required"]
async fn configured_s3_curl_transport_control() {
    assert_eq!(std::env::var("OTACAST_RUN_LIVE_S3").as_deref(), Ok("1"));
    let mut settings = S3Config::load().unwrap();
    settings.public_base_url = None;
    let dir = tempfile::tempdir().unwrap();
    let (mut task, md5) = super::sizes::sized_task(&dir, 8 * 1024 * 1024).await;
    let unique = format!(
        "{:032x}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    task.id = format!(
        "{}-{}-{}-{}-{}",
        &unique[..8],
        &unique[8..12],
        &unique[12..16],
        &unique[16..20],
        &unique[20..]
    );
    task.key = format!("_otacast-tests/{}/curl-control.bin", task.id);
    task.target = Target::from(&settings);
    task.file_url = object_url::object_url(&settings, &task.key).unwrap();
    let url = url::Url::parse(&task.file_url).unwrap();
    let quote = |value: &str| {
        format!(
            "\"{}\"",
            value
                .replace('\\', "\\\\")
                .replace('"', "\\\"")
                .replace('\n', "\\n")
                .replace('\r', "\\r")
        )
    };
    // Credentials go to curl through stdin, never argv, shell expansion or logs.
    let mut config = format!("url = {}\naws-sigv4 = {}\nuser = {}\nupload-file = {}\nheader = {}\nheader = {}\noutput = {}\n",
        quote(url.as_str()), quote(&format!("aws:amz:{}:s3", settings.region)),
        quote(&format!("{}:{}", settings.access_key, settings.secret_key)), quote(&task.file_path),
        quote(&format!("x-amz-meta-otacast-task: {}", task.id)), quote(&format!("Content-MD5: {}", STANDARD.encode(hex::decode(&md5).unwrap()))),
        quote(dir.path().join("response.xml").to_str().unwrap()));
    if let Some(token) = &settings.session_token {
        config.push_str(&format!(
            "header = {}\n",
            quote(&format!("x-amz-security-token: {token}"))
        ));
    }
    if let Some(ip) = settings.connect_ips.first() {
        config.push_str(&format!(
            "resolve = {}\n",
            quote(&format!(
                "{}:{}:{ip}",
                url.host_str().unwrap(),
                url.port_or_known_default().unwrap()
            ))
        ));
    }
    let mut command = tokio::process::Command::new("curl");
    command.args(["--config", "-", "--silent", "--show-error", "--fail-with-body", "--noproxy", "*", "--max-time", "90", "--write-out", "curl_control http=%{http_code} version=%{http_version} remote=%{remote_ip} bytes=%{size_upload} speed=%{speed_upload} total=%{time_total}s\\n"]);
    if std::env::var("OTACAST_CURL_HTTP1").as_deref() == Ok("1") {
        command.arg("--http1.1");
    }
    let mut child = command
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(config.as_bytes())
        .await
        .unwrap();
    let output = child.wait_with_output().await.unwrap();
    println!("{}", String::from_utf8_lossy(&output.stdout));
    let client = client(&settings);
    let verified = if output.status.success() {
        download_and_verify(&client, &task, &md5).await
    } else {
        Err(format!(
            "curl transport failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ))
    };
    cleanup_live(&client, &task, &store_at(&dir)).await.unwrap();
    println!("curl_control_cleanup=confirmed");
    verified.unwrap();
}
