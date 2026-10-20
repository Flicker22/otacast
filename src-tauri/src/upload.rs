#[cfg(test)]
use crate::upload_store::{CONCURRENCY, PART_SIZE};
use crate::{
    get_file_info_impl, object_url,
    s3_config::S3Config,
    upload_progress::track_upload,
    upload_store::{Part, Store, Target, Task, TaskList, MULTIPART_THRESHOLD},
    UploadResult,
};
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::{
    config::{retry::RetryConfig, timeout::TimeoutConfig, BehaviorVersion, Credentials, Region},
    primitives::ByteStream,
    types::{
        ChecksumAlgorithm, ChecksumMode, ChecksumType, CompletedMultipartUpload, CompletedPart,
        ObjectCannedAcl,
    },
    Client,
};
use aws_smithy_types::byte_stream::Length;
use base64::{engine::general_purpose::STANDARD, Engine};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};
use tauri::{ipc::Channel, Manager, State};
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt},
    sync::watch,
    task::JoinSet,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Control {
    Run,
    Pause,
    Cancel,
}

struct Active {
    id: String,
    control: watch::Sender<Control>,
    sealed: bool,
}
#[derive(Default)]
pub struct UploadManager {
    active: Mutex<Option<Active>>,
}

struct Lease<'a> {
    manager: &'a UploadManager,
    receiver: watch::Receiver<Control>,
}
impl Drop for Lease<'_> {
    fn drop(&mut self) {
        *self.manager.active.lock().unwrap() = None;
    }
}
impl UploadManager {
    fn acquire(&self, id: &str) -> Result<Lease<'_>, String> {
        let mut active = self.active.lock().unwrap();
        if active.is_some() {
            return Err("已有上传任务正在运行".into());
        }
        let (control, receiver) = watch::channel(Control::Run);
        *active = Some(Active {
            id: id.into(),
            control,
            sealed: false,
        });
        Ok(Lease {
            manager: self,
            receiver,
        })
    }
    // Serializes the cancellation/commit boundary with control_upload.
    fn seal(&self) -> Result<(), Failure> {
        let mut active = self.active.lock().unwrap();
        let active = active.as_mut().ok_or("上传任务已结束")?;
        check_control(&active.control.subscribe())?;
        active.sealed = true;
        Ok(())
    }
    fn signal(&self, id: &str, action: Control) -> Result<(), String> {
        let active = self.active.lock().unwrap();
        let active = active
            .as_ref()
            .filter(|a| a.id == id)
            .ok_or("任务未在运行，请从恢复列表操作")?;
        if active.sealed {
            return Err("正在提交或校验对象，请等待结果".into());
        }
        if *active.control.borrow() != Control::Run {
            return Err("已收到控制请求，正在保存任务或清理分片".into());
        }
        active.control.send(action).map_err(|_| "任务已结束".into())
    }
}

#[derive(Debug)]
enum Failure {
    Pause,
    Cancel,
    Error(String),
}
impl From<String> for Failure {
    fn from(s: String) -> Self {
        Self::Error(s)
    }
}
impl From<&str> for Failure {
    fn from(s: &str) -> Self {
        Self::Error(s.into())
    }
}
impl From<std::io::Error> for Failure {
    fn from(e: std::io::Error) -> Self {
        Self::Error(e.to_string())
    }
}
type Result<T, E = Failure> = std::result::Result<T, E>;

fn check_control(rx: &watch::Receiver<Control>) -> Result<()> {
    match *rx.borrow() {
        Control::Run => Ok(()),
        Control::Pause => Err(Failure::Pause),
        Control::Cancel => Err(Failure::Cancel),
    }
}
async fn interruptible<T>(
    rx: &watch::Receiver<Control>,
    future: impl Future<Output = Result<T>>,
) -> Result<T> {
    check_control(rx)?;
    let mut changed = rx.clone();
    tokio::select! {
        biased;
        _ = changed.changed() => { check_control(&changed)?; Err("上传控制通道已关闭".into()) },
        result = future => result,
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct Progress {
    pub task_id: String,
    pub uploaded_bytes: u64,
    pub total_bytes: u64,
    pub phase: String,
}
type Observer = Arc<dyn Fn(Progress) + Send + Sync>;
struct Counters {
    confirmed: BTreeMap<i32, u64>,
    sending: BTreeMap<i32, u64>,
    phase: String,
}
struct ProgressTracker {
    id: String,
    total: u64,
    counters: Mutex<Counters>,
    observer: Observer,
}
impl ProgressTracker {
    fn new(task: &Task, observer: Observer) -> Arc<Self> {
        Arc::new(Self {
            id: task.id.clone(),
            total: task.file_info.file_size,
            counters: Mutex::new(Counters {
                confirmed: BTreeMap::new(),
                sending: BTreeMap::new(),
                phase: "preparing".into(),
            }),
            observer,
        })
    }
    fn update(&self, change: impl FnOnce(&mut Counters)) {
        let mut c = self.counters.lock().unwrap();
        change(&mut c);
        let uploaded = c.confirmed.values().sum::<u64>()
            + c.sending
                .iter()
                .filter(|(n, _)| !c.confirmed.contains_key(n))
                .map(|(_, b)| b)
                .sum::<u64>();
        (self.observer)(Progress {
            task_id: self.id.clone(),
            uploaded_bytes: uploaded.min(self.total),
            total_bytes: self.total,
            phase: c.phase.clone(),
        });
    }
    fn phase(&self, phase: &str) {
        self.update(|c| c.phase = phase.into());
    }
    fn sent(&self, number: i32, bytes: u64) {
        self.update(|c| {
            c.sending.insert(number, bytes);
        });
    }
    fn confirm(&self, part: &Part) {
        self.update(|c| {
            c.sending.remove(&part.number);
            c.confirmed.insert(part.number, part.size);
        });
    }
}

pub fn client(settings: &S3Config) -> Client {
    let mut config = aws_sdk_s3::config::Builder::new()
        .region(Region::new(settings.region.clone()))
        .credentials_provider(Credentials::new(
            settings.access_key.clone(),
            settings.secret_key.clone(),
            settings.session_token.clone(),
            None,
            "s3-env-config",
        ))
        .force_path_style(settings.force_path_style)
        .behavior_version(BehaviorVersion::latest())
        .stalled_stream_protection(
            aws_sdk_s3::config::StalledStreamProtectionConfig::enabled()
                .grace_period(Duration::from_secs(settings.stall_grace_seconds))
                .build(),
        )
        // The SDK owns retry classification, exponential backoff and jitter.
        .retry_config(RetryConfig::standard().with_max_attempts(5))
        .timeout_config(
            TimeoutConfig::builder()
                .connect_timeout(Duration::from_secs(15))
                .operation_attempt_timeout(Duration::from_secs(15 * 60))
                .operation_timeout(Duration::from_secs(90 * 60))
                .build(),
        )
        .request_checksum_calculation(aws_sdk_s3::config::RequestChecksumCalculation::WhenRequired);
    if let Some(endpoint) = &settings.endpoint {
        config = config.endpoint_url(endpoint);
    }
    if let Some(http_client) = crate::s3_transport::http_client(settings) {
        config = config.http_client(http_client);
    }
    Client::from_conf(config.build())
}

fn aws_error<E: aws_sdk_s3::error::ProvideErrorMetadata + std::fmt::Debug>(
    context: &str,
    error: aws_sdk_s3::error::SdkError<E>,
) -> Failure {
    // Do not persist Debug request dumps: they may contain signed headers.
    let detail = if let Some(service) = error.as_service_error() {
        format!(
            "{}: {}",
            service.code().unwrap_or("Unknown"),
            service.message().unwrap_or("服务端未提供错误详情")
        )
    } else if let aws_sdk_s3::error::SdkError::DispatchFailure(dispatch) = &error {
        let mut detail = "网络请求发送失败";
        if let Some(connector) = dispatch.as_connector_error() {
            let mut source: Option<&(dyn std::error::Error + 'static)> = Some(connector);
            while let Some(cause) = source {
                let message = cause.to_string().to_ascii_lowercase();
                if message.contains("minimum throughput") {
                    detail = "上传流长时间无进展，触发停滞保护；请检查代理、网关或网络连接";
                    break;
                } else if message.contains("connection reset") {
                    detail = "连接被对端重置；请检查代理、网关或网络连接";
                    break;
                } else if message.contains("broken pipe") {
                    detail = "发送连接已断开（Broken pipe），任务已保留";
                    break;
                } else if message.contains("certificate") {
                    detail = "TLS 证书验证失败，请检查存储域名和证书";
                    break;
                } else if message.contains("dns") {
                    detail = "存储域名解析失败，请检查 DNS 或 S3_CONNECT_IPS";
                } else if message.contains("timeout") || message.contains("timed out") {
                    detail = "网络连接超时，任务已保留";
                }
                source = cause.source();
            }
        }
        // Persist classified causes only: raw source strings can contain URLs
        // or credentials. Keep the SDK category for support/debugging.
        format!("{detail}（dispatch failure）")
    } else {
        error.to_string()
    };
    Failure::Error(format!("{context}: {detail}"))
}

async fn metadata_request<T>(future: impl Future<Output = Result<T>>) -> Result<T> {
    tokio::time::timeout(Duration::from_secs(60), future)
        .await
        .map_err(|_| Failure::Error("远端状态查询超时，任务已保留".into()))?
}

async fn hash_range(path: &str, offset: u64, size: u64) -> Result<String> {
    let mut file = tokio::fs::File::open(path).await?;
    file.seek(std::io::SeekFrom::Start(offset)).await?;
    let mut remaining = size;
    let mut hash = Sha256::new();
    let mut buffer = vec![0; 64 * 1024];
    while remaining > 0 {
        let n = file
            .read(&mut buffer[..remaining.min(64 * 1024) as usize])
            .await?;
        if n == 0 {
            return Err("文件读取提前结束，请检查文件是否发生变化".into());
        }
        hash.update(&buffer[..n]);
        remaining -= n as u64;
    }
    Ok(STANDARD.encode(hash.finalize()))
}

fn expected_checksum(task: &Task) -> Result<String> {
    if task.parts.len() != task.part_count() as usize {
        return Err("缺少已完成分片的校验记录".into());
    }
    if !task.multipart {
        return Ok(task.parts.get(&1).ok_or("缺少文件校验记录")?.sha256.clone());
    }
    let mut composite = Sha256::new();
    for n in 1..=task.part_count() {
        let digest = STANDARD
            .decode(&task.parts.get(&n).ok_or("分片不连续")?.sha256)
            .map_err(|_| "分片摘要格式无效")?;
        if digest.len() != 32 {
            return Err("分片摘要长度无效".into());
        }
        composite.update(digest);
    }
    Ok(format!(
        "{}-{}",
        STANDARD.encode(composite.finalize()),
        task.part_count()
    ))
}

async fn unchanged(task: &Task) -> Result<()> {
    if get_file_info_impl(&task.file_path)
        .await
        .map_err(|e| e.to_string())?
        != task.file_info
    {
        return Err("本地文件已变化，请放弃此任务并重新上传".into());
    }
    Ok(())
}

fn store_for(app: &tauri::AppHandle) -> Result<Store, String> {
    Ok(Store {
        root: app
            .path()
            .app_data_dir()
            .map_err(|e| e.to_string())?
            .join("uploads-v1"),
    })
}
fn writer_lock(store: &Store) -> Result<std::fs::File, String> {
    std::fs::create_dir_all(&store.root).map_err(|e| e.to_string())?;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(store.root.join("runner.lock"))
        .map_err(|e| e.to_string())?;
    file.try_lock()
        .map_err(|_| "另一个应用实例正在操作上传任务".to_string())?;
    Ok(file)
}

// Returns None only for a missing object or a different task's previous object.
async fn probe(
    client: &Client,
    task: &Task,
) -> Result<Option<aws_sdk_s3::operation::head_object::HeadObjectOutput>> {
    let mut request = client
        .head_object()
        .bucket(&task.target.bucket)
        .key(&task.key)
        .set_version_id(task.version_id.clone());
    if task.checksum && task.remote_checksums {
        request = request.checksum_mode(ChecksumMode::Enabled);
    }
    let response = metadata_request(async {
        match request.send().await {
            Ok(head) => Ok(Some(head)),
            Err(e)
                if e.as_service_error().is_some_and(|s| s.is_not_found())
                    || e.raw_response().is_some_and(|r| r.status().as_u16() == 404) =>
            {
                Ok(None)
            }
            Err(e) => Err(aws_error("无法查询远端对象（请检查读取与校验权限）", e)),
        }
    })
    .await?;
    let Some(head) = response else {
        return Ok(None);
    };
    if head.metadata().and_then(|m| m.get("otacast-task")) != Some(&task.id) {
        return Ok(None);
    }
    Ok(Some(head))
}

fn verify_head(
    task: &Task,
    head: &aws_sdk_s3::operation::head_object::HeadObjectOutput,
) -> Result<String> {
    if head.content_length() != Some(task.file_info.file_size as i64) {
        return Err("远端文件大小不一致，已保留任务供排查".into());
    }
    if task
        .etag
        .as_ref()
        .is_some_and(|e| head.e_tag() != Some(e.as_str()))
    {
        return Err("远端对象 ETag 已变化，已停止校验".into());
    }
    if task.checksum {
        let expected = expected_checksum(task)?;
        match head.checksum_sha256() {
            None => return Ok("download_sha256".into()),
            Some(actual) if actual != expected => {
                return Err("远端 SHA-256 摘要与本地不一致，已保留任务供排查".into());
            }
            Some(_) => {}
        }
        if head.checksum_type().is_some_and(|kind| {
            kind != &if task.multipart {
                ChecksumType::Composite
            } else {
                ChecksumType::FullObject
            }
        }) {
            return Err("远端 checksum 类型与上传方式不匹配".into());
        }
        Ok(if task.multipart {
            "sha256_composite"
        } else {
            "sha256"
        }
        .into())
    } else {
        Ok("size_only".into())
    }
}

async fn verify_download(
    client: &Client,
    task: &Task,
    head: &aws_sdk_s3::operation::head_object::HeadObjectOutput,
) -> Result<()> {
    // Verify against the hashes recorded before upload, not a metadata field
    // echoed by the service. Version and ETag pin the GET to this exact object.
    expected_checksum(task)?;
    let response = client
        .get_object()
        .bucket(&task.target.bucket)
        .key(&task.key)
        .set_version_id(head.version_id().map(str::to_string))
        .set_if_match(head.e_tag().map(str::to_string))
        .send()
        .await
        .map_err(|e| aws_error("下载内容校验失败，任务已保留", e))?;
    if response.metadata().and_then(|m| m.get("otacast-task")) != Some(&task.id)
        || response.content_length() != Some(task.file_info.file_size as i64)
        || response.e_tag() != head.e_tag()
        || (head.version_id().is_some() && response.version_id() != head.version_id())
    {
        return Err("下载校验的对象标识、版本或大小不一致，已停止校验".into());
    }
    let mut body = response.body;
    let mut pending = bytes::Bytes::new();
    for number in 1..=task.part_count() {
        let part = task.parts.get(&number).ok_or("缺少分片校验记录")?;
        let mut remaining = part.size;
        let mut hash = Sha256::new();
        while remaining > 0 {
            if pending.is_empty() {
                pending = tokio::time::timeout(Duration::from_secs(120), body.next())
                    .await
                    .map_err(|_| "下载校验超过 120 秒无数据，任务已保留")?
                    .ok_or("下载校验数据提前结束，任务已保留")?
                    .map_err(|e| format!("下载校验读取失败，任务已保留: {e}"))?;
                if pending.is_empty() {
                    continue;
                }
            }
            let take = remaining.min(pending.len() as u64) as usize;
            hash.update(&pending[..take]);
            pending = pending.slice(take..);
            remaining -= take as u64;
        }
        if STANDARD.encode(hash.finalize()) != part.sha256 {
            return Err(format!("下载内容 SHA-256 校验失败（分片 {number}），任务已保留").into());
        }
    }
    if !pending.is_empty() {
        return Err("下载内容超出预期大小，任务已保留".into());
    }
    while let Some(chunk) = tokio::time::timeout(Duration::from_secs(120), body.next())
        .await
        .map_err(|_| "下载校验结束等待超时，任务已保留")?
    {
        if !chunk
            .map_err(|e| format!("下载校验读取失败: {e}"))?
            .is_empty()
        {
            return Err("下载内容超出预期大小，任务已保留".into());
        }
    }
    Ok(())
}

async fn output(
    client: &Client,
    task: &Task,
    head: &aws_sdk_s3::operation::head_object::HeadObjectOutput,
    progress: &ProgressTracker,
) -> Result<UploadResult> {
    let verification = verify_head(task, head)?;
    if verification == "download_sha256" {
        progress.phase("verifying_download");
        verify_download(client, task, head).await?;
    }
    Ok(UploadResult {
        file_info: task.file_info.clone(),
        s3_key: task.key.clone(),
        file_url: task.file_url.clone(),
        etag: head.e_tag().unwrap_or_default().into(),
        verification,
    })
}

async fn upload_part(
    client: Client,
    task: Task,
    number: i32,
    progress: Arc<ProgressTracker>,
    rx: watch::Receiver<Control>,
) -> Result<Part> {
    interruptible(&rx, async {
        let (offset, size) = task.range(number);
        let sha256 = hash_range(&task.file_path, offset, size).await?;
        let body = ByteStream::read_from()
            .path(&task.file_path)
            .offset(offset)
            .length(Length::Exact(size))
            .buffer_size(64 * 1024)
            .build()
            .await
            .map_err(|e| e.to_string())?;
        let body = track_upload(
            body,
            size,
            Arc::new(move |p| progress.sent(number, p.uploaded_bytes)),
        );
        let mut request = client
            .upload_part()
            .bucket(&task.target.bucket)
            .key(&task.key)
            .upload_id(task.upload_id.as_deref().ok_or("缺少上传 ID")?)
            .part_number(number)
            .content_length(size as i64)
            .body(body);
        if task.checksum && task.remote_checksums {
            request = request
                .checksum_algorithm(ChecksumAlgorithm::Sha256)
                .checksum_sha256(&sha256);
        }
        let response = request
            .send()
            .await
            .map_err(|e| aws_error(&format!("分片 {number} 上传失败（可继续任务）"), e))?;
        if task.checksum && response.checksum_sha256().is_some_and(|s| s != sha256) {
            return Err("服务端返回的分片摘要不一致".into());
        }
        Ok(Part {
            number,
            size,
            etag: response.e_tag().ok_or("服务端没有返回分片 ETag")?.into(),
            sha256,
        })
    })
    .await
}

async fn reconcile(
    client: &Client,
    task: &mut Task,
    store: &Store,
    progress: &ProgressTracker,
    rx: &watch::Receiver<Control>,
) -> Result<()> {
    progress.phase("checking_local");
    unchanged(task).await?;
    let mut remote = BTreeMap::new();
    let mut marker = None;
    loop {
        let page = interruptible(
            rx,
            metadata_request(async {
                client
                    .list_parts()
                    .bucket(&task.target.bucket)
                    .key(&task.key)
                    .upload_id(task.upload_id.as_deref().ok_or("缺少上传 ID")?)
                    .set_part_number_marker(marker.clone())
                    .send()
                    .await
                    .map_err(|e| {
                        aws_error("无法恢复远端分片（任务可能已过期或缺少 ListParts 权限）", e)
                    })
            }),
        )
        .await?;
        for part in page.parts() {
            if let Some(n) = part.part_number() {
                remote.insert(n, part.clone());
            }
        }
        if !page.is_truncated().unwrap_or(false) {
            break;
        }
        let next = page
            .next_part_number_marker()
            .map(str::to_string)
            .ok_or("远端分片分页标记缺失")?;
        if marker.as_ref() == Some(&next) {
            return Err("远端分片分页标记未推进".into());
        }
        marker = Some(next);
    }
    let mut retained = BTreeMap::new();
    for (n, part) in &task.parts {
        check_control(rx)?;
        let (offset, size) = task.range(*n);
        let local = interruptible(rx, hash_range(&task.file_path, offset, size)).await?;
        if local != part.sha256 {
            return Err("已上传部分的本地内容发生变化，请放弃任务并重新上传".into());
        }
        if let Some(r) = remote.get(n) {
            if r.e_tag() == Some(part.etag.as_str())
                && r.size() == Some(size as i64)
                && (!task.checksum
                    || r.checksum_sha256()
                        .is_none_or(|digest| digest == part.sha256))
            {
                retained.insert(*n, part.clone());
                progress.confirm(part);
            }
        }
        // Unjournaled remote parts are intentionally re-uploaded, never trusted.
    }
    task.parts = retained;
    unchanged(task).await?;
    store.save(task)?;
    Ok(())
}

async fn multipart(
    client: &Client,
    task: &mut Task,
    store: &Store,
    progress: Arc<ProgressTracker>,
    rx: &watch::Receiver<Control>,
    manager: &UploadManager,
) -> Result<()> {
    if task.upload_id.is_none() {
        check_control(rx)?;
        let mut request = client
            .create_multipart_upload()
            .bucket(&task.target.bucket)
            .key(&task.key)
            .acl(ObjectCannedAcl::PublicRead)
            .metadata("otacast-task", &task.id);
        if task.checksum && task.remote_checksums {
            request = request
                .checksum_algorithm(ChecksumAlgorithm::Sha256)
                .checksum_type(ChecksumType::Composite);
        }
        // Do not drop creation on pause: first persist its returned upload ID.
        let response = tokio::time::timeout(Duration::from_secs(90), request.send())
            .await
            .map_err(|_| "创建分片任务超时，请配置桶端过期清理作为兜底")?
            .map_err(|e| {
                aws_error(
                    "无法创建分片上传（兼容服务需支持 SHA-256，否则请明确关闭远端校验）",
                    e,
                )
            })?;
        task.upload_id = Some(response.upload_id().ok_or("服务端未返回上传 ID")?.into());
        store.save(task)?;
    } else {
        reconcile(client, task, store, &progress, rx).await?;
    }
    task.phase = "uploading".into();
    store.save(task)?;
    progress.phase("uploading");
    let pending: Vec<i32> = (1..=task.part_count())
        .filter(|n| !task.parts.contains_key(n))
        .collect();
    let mut pending = pending.into_iter();
    let mut workers = JoinSet::new();
    let result: Result<()> = async {
        loop {
            check_control(rx)?;
            while workers.len() < task.concurrency {
                let Some(n) = pending.next() else {
                    break;
                };
                workers.spawn(upload_part(
                    client.clone(),
                    task.clone(),
                    n,
                    progress.clone(),
                    rx.clone(),
                ));
            }
            let Some(result) = workers.join_next().await else {
                break;
            };
            let part = result.map_err(|e| format!("分片任务异常: {e}"))??;
            task.parts.insert(part.number, part.clone());
            store.save(task)?;
            progress.confirm(&part);
        }
        Ok(())
    }
    .await;
    // Drain cancelled requests before calling AbortMultipartUpload.
    if result.is_err() {
        workers.abort_all();
        while workers.join_next().await.is_some() {}
    }
    result?;
    unchanged(task).await?;
    manager.seal()?;
    task.phase = "completing".into();
    store.save(task)?;
    progress.phase("completing");
    let parts = task
        .parts
        .values()
        .map(|p| {
            let mut part = CompletedPart::builder()
                .part_number(p.number)
                .e_tag(&p.etag);
            if task.checksum && task.remote_checksums {
                part = part.checksum_sha256(&p.sha256);
            }
            part.build()
        })
        .collect();
    let mut request = client
        .complete_multipart_upload()
        .bucket(&task.target.bucket)
        .key(&task.key)
        .upload_id(task.upload_id.as_deref().ok_or("缺少上传 ID")?)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(parts))
                .build(),
        );
    if task.checksum && task.remote_checksums {
        request = request
            .checksum_type(ChecksumType::Composite)
            .checksum_sha256(expected_checksum(task)?);
    }
    match tokio::time::timeout(Duration::from_secs(180), request.send()).await {
        Ok(Ok(response)) => {
            task.etag = response.e_tag().map(str::to_string);
            task.version_id = response.version_id().map(str::to_string);
        }
        error => {
            // The request may have committed even though the response was lost.
            let cause = match error {
                Ok(Err(error)) => match aws_error("合并请求失败", error) {
                    Failure::Error(message) => message,
                    _ => unreachable!(),
                },
                _ => "合并请求超时".to_string(),
            };
            let Some(head) = probe(client, task).await? else {
                return Err(
                    format!("{cause}；合并结果尚未确认，任务已保留，请继续任务以重新查询").into(),
                );
            };
            verify_head(task, &head)?;
            task.etag = head.e_tag().map(str::to_string);
            task.version_id = head.version_id().map(str::to_string);
        }
    }
    Ok(())
}

async fn single(
    client: &Client,
    task: &mut Task,
    store: &Store,
    progress: Arc<ProgressTracker>,
    rx: &watch::Receiver<Control>,
) -> Result<()> {
    progress.phase("checking_local");
    let sha256 =
        interruptible(rx, hash_range(&task.file_path, 0, task.file_info.file_size)).await?;
    task.parts.insert(
        1,
        Part {
            number: 1,
            size: task.file_info.file_size,
            etag: String::new(),
            sha256: sha256.clone(),
        },
    );
    // Persist before sending so a lost response can be reconciled after restart.
    task.phase = "completing".into();
    store.save(task)?;
    unchanged(task).await?;
    progress.phase("uploading");
    let body = ByteStream::read_from()
        .path(&task.file_path)
        .buffer_size(64 * 1024)
        .length(Length::Exact(task.file_info.file_size))
        .build()
        .await
        .map_err(|e| e.to_string())?;
    let body = track_upload(
        body,
        task.file_info.file_size,
        Arc::new(move |p| progress.sent(1, p.uploaded_bytes)),
    );
    let mut request = client
        .put_object()
        .bucket(&task.target.bucket)
        .key(&task.key)
        .acl(ObjectCannedAcl::PublicRead)
        .content_length(task.file_info.file_size as i64)
        .metadata("otacast-task", &task.id)
        .body(body);
    if task.checksum && task.remote_checksums {
        request = request
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .checksum_sha256(sha256);
    }
    let response = interruptible(rx, async {
        request.send().await.map_err(|e| aws_error("上传失败", e))
    })
    .await?;
    task.etag = response.e_tag().map(str::to_string);
    task.version_id = response.version_id().map(str::to_string);
    unchanged(task).await?;
    Ok(())
}

async fn execute(
    client: &Client,
    task: &mut Task,
    store: &Store,
    progress: Arc<ProgressTracker>,
    rx: &watch::Receiver<Control>,
    manager: &UploadManager,
) -> Result<UploadResult> {
    if matches!(task.phase.as_str(), "completing" | "verifying") {
        progress.phase("verifying");
        if let Some(head) = interruptible(rx, probe(client, task)).await? {
            manager.seal()?;
            return output(client, task, &head, &progress).await;
        }
        if task.phase == "verifying" {
            return Err("已经提交的远端对象已丢失或被覆盖，任务已保留".into());
        }
    }
    unchanged(task).await?;
    if task.multipart {
        multipart(client, task, store, progress.clone(), rx, manager).await?;
    } else {
        single(client, task, store, progress.clone(), rx).await?;
        manager.seal()?;
    }
    task.phase = "verifying".into();
    store.save(task)?;
    progress.phase("verifying");
    let head = probe(client, task)
        .await?
        .ok_or("远端对象不属于此次任务或已经被覆盖，已停止校验")?;
    output(client, task, &head, &progress).await
}

async fn cleanup(client: &Client, task: &Task, store: &Store) -> Result<(), String> {
    if let Some(upload_id) = &task.upload_id {
        let mut removed = false;
        for _ in 0..3 {
            let response = tokio::time::timeout(
                Duration::from_secs(60),
                client
                    .abort_multipart_upload()
                    .bucket(&task.target.bucket)
                    .key(&task.key)
                    .upload_id(upload_id)
                    .send(),
            )
            .await
            .map_err(|_| "远端分片清理超时，下次启动会重试".to_string())?;
            match response {
                Ok(_) => {}
                Err(e)
                    if e.as_service_error()
                        .is_some_and(|s| s.code() == Some("NoSuchUpload")) =>
                {
                    removed = true;
                    break;
                }
                Err(e) => {
                    return Err(match aws_error("远端分片清理失败，下次启动会重试", e)
                    {
                        Failure::Error(s) => s,
                        _ => unreachable!(),
                    })
                }
            }
            // Requests already accepted by the service may outlive a disconnected
            // client. Confirm that the upload is gone before dropping the journal.
            let status = tokio::time::timeout(
                Duration::from_secs(60),
                client
                    .list_parts()
                    .bucket(&task.target.bucket)
                    .key(&task.key)
                    .upload_id(upload_id)
                    .max_parts(1)
                    .send(),
            )
            .await
            .map_err(|_| "清理后的状态查询超时，已保留待清理记录".to_string())?;
            match status {
                Err(e)
                    if e.as_service_error()
                        .is_some_and(|s| s.code() == Some("NoSuchUpload")) =>
                {
                    removed = true;
                    break;
                }
                Err(e) => {
                    return Err(match aws_error("无法确认分片已清理，已保留待清理记录", e)
                    {
                        Failure::Error(s) => s,
                        _ => unreachable!(),
                    })
                }
                Ok(_) => {}
            }
        }
        if !removed {
            return Err("远端上传仍存在，已保留记录供下次继续清理".into());
        }
    }
    // Never delete a committed object: it may be a published version.
    store.remove(&task.id)
}

#[derive(Debug, Serialize)]
pub struct Outcome {
    status: String,
    result: Option<UploadResult>,
}

async fn run(
    client: Client,
    mut task: Task,
    store: Store,
    observer: Observer,
    lease: &Lease<'_>,
) -> Result<Outcome, String> {
    let progress = ProgressTracker::new(&task, observer);
    let result = execute(
        &client,
        &mut task,
        &store,
        progress.clone(),
        &lease.receiver,
        lease.manager,
    )
    .await;
    match result {
        Ok(result) => {
            store.remove(&task.id)?;
            progress.update(|c| {
                c.confirmed.clear();
                c.sending.clear();
                c.confirmed.insert(0, task.file_info.file_size);
                c.phase = "complete".into();
            });
            Ok(Outcome {
                status: "complete".into(),
                result: Some(result),
            })
        }
        Err(Failure::Cancel) => {
            task.phase = "cleanup_pending".into();
            task.last_error = None;
            store.save(&task)?;
            progress.phase("cancelling");
            if let Err(error) = cleanup(&client, &task, &store).await {
                task.last_error = Some(error.clone());
                store.save(&task)?;
                return Err(error);
            }
            progress.phase("cancelled");
            Ok(Outcome {
                status: "cancelled".into(),
                result: None,
            })
        }
        Err(Failure::Pause) => {
            // Keep the commit marker for an interrupted single PUT.
            if task.phase != "completing" {
                task.phase = "paused".into();
            }
            task.last_error = None;
            store.save(&task)?;
            progress.phase("paused");
            Ok(Outcome {
                status: "paused".into(),
                result: None,
            })
        }
        Err(Failure::Error(error)) => {
            if !matches!(task.phase.as_str(), "completing" | "verifying") {
                task.phase = "failed".into();
            }
            task.last_error = Some(error.clone());
            store.save(&task)?;
            progress.phase("failed");
            Err(error)
        }
    }
}

#[tauri::command]
pub async fn upload_file_to_s3(
    app: tauri::AppHandle,
    manager: State<'_, UploadManager>,
    task_id: String,
    file_path: String,
    key: String,
    checksum: bool,
    on_progress: Channel<Progress>,
) -> Result<Outcome, String> {
    crate::upload_store::validate_id(&task_id)?;
    let lease = manager.acquire(&task_id)?;
    let store = store_for(&app)?;
    let _lock = writer_lock(&store)?;
    if store.root.join(format!("{task_id}.json")).exists() {
        return Err("任务 ID 已存在，请使用继续上传".into());
    }
    let settings = S3Config::load()?;
    let file_path = tokio::fs::canonicalize(file_path)
        .await
        .map_err(|e| e.to_string())?
        .to_str()
        .ok_or("文件路径必须是 UTF-8")?
        .to_string();
    let info = get_file_info_impl(&file_path)
        .await
        .map_err(|e| e.to_string())?;
    let task = Task {
        schema: 1,
        id: task_id,
        file_path,
        multipart: info.file_size > MULTIPART_THRESHOLD,
        file_info: info,
        target: Target::from(&settings),
        file_url: object_url::object_url(&settings, &key)?,
        key,
        checksum,
        remote_checksums: settings.send_checksum_headers,
        part_size: settings.part_size,
        concurrency: settings.upload_concurrency,
        upload_id: None,
        parts: BTreeMap::new(),
        phase: "preparing".into(),
        last_error: None,
        etag: None,
        version_id: None,
    };
    store.save(&task)?;
    run(
        client(&settings),
        task,
        store,
        Arc::new(move |p| {
            let _ = on_progress.send(p);
        }),
        &lease,
    )
    .await
}

#[tauri::command]
pub async fn resume_upload(
    app: tauri::AppHandle,
    manager: State<'_, UploadManager>,
    task_id: String,
    on_progress: Channel<Progress>,
) -> Result<Outcome, String> {
    let lease = manager.acquire(&task_id)?;
    let store = store_for(&app)?;
    let _lock = writer_lock(&store)?;
    let mut task = store.load(&task_id)?;
    if task.phase == "cleanup_pending" {
        return Err("此任务已取消，请执行放弃并清理".into());
    }
    let settings = S3Config::load()?;
    if task.target != Target::from(&settings) {
        return Err(
            "当前存储目标与原任务不同，请恢复原 Bucket、Region、Endpoint 和寻址方式后继续".into(),
        );
    }
    task.file_url = object_url::object_url(&settings, &task.key)?;
    task.concurrency = settings.upload_concurrency;
    run(
        client(&settings),
        task,
        store,
        Arc::new(move |p| {
            let _ = on_progress.send(p);
        }),
        &lease,
    )
    .await
}

#[tauri::command]
pub fn control_upload(
    manager: State<'_, UploadManager>,
    task_id: String,
    action: String,
) -> Result<(), String> {
    let action = match action.as_str() {
        "pause" => Control::Pause,
        "cancel" => Control::Cancel,
        _ => return Err("无效的控制操作".into()),
    };
    manager.signal(&task_id, action)
}

#[tauri::command]
pub async fn discard_upload(
    app: tauri::AppHandle,
    manager: State<'_, UploadManager>,
    task_id: String,
) -> Result<(), String> {
    let _lease = manager.acquire(&task_id)?;
    let store = store_for(&app)?;
    let _lock = writer_lock(&store)?;
    let mut task = store.load(&task_id)?;
    discard_task(&mut task, &store, S3Config::load).await
}

async fn discard_task(
    task: &mut Task,
    store: &Store,
    load_settings: impl FnOnce() -> Result<S3Config, String>,
) -> Result<(), String> {
    // Single PUTs and multipart tasks without a saved upload ID have no known
    // remote multipart upload to abort. Removing their journal needs no config.
    // Already committed objects are always preserved, just as in cleanup().
    if task.upload_id.is_none() {
        return store.remove(&task.id);
    }
    let settings = load_settings()?;
    if task.target != Target::from(&settings) {
        return Err(format!(
            "此任务有远端分片，请将 .env 恢复为 S3_BUCKET={}、S3_REGION={}、S3_ENDPOINT={}、S3_FORCE_PATH_STYLE={}，重新加载配置后清理",
            task.target.bucket,
            task.target.region,
            task.target.endpoint.as_deref().unwrap_or("（留空，使用 AWS 默认地址）"),
            task.target.force_path_style,
        ));
    }
    task.phase = "cleanup_pending".into();
    store.save(task)?;
    cleanup(&client(&settings), task, store).await
}

#[tauri::command]
pub async fn list_upload_tasks(
    app: tauri::AppHandle,
    manager: State<'_, UploadManager>,
) -> Result<TaskList, String> {
    let store = store_for(&app)?;
    // Retry cancelled uploads on startup when the original target is configured.
    if let Ok(_lease) = manager.acquire("cleanup") {
        if let Ok(_lock) = writer_lock(&store) {
            if let Ok(settings) = S3Config::load() {
                let client = client(&settings);
                for mut task in store.list()?.tasks {
                    if task.phase == "cleanup_pending" && task.target == Target::from(&settings) {
                        if let Err(error) = cleanup(&client, &task, &store).await {
                            task.last_error = Some(error);
                            store.save(&task)?;
                        }
                    }
                }
            }
        }
    }
    store.list()
}

#[cfg(test)]
#[path = "upload_tests.rs"]
mod tests;
