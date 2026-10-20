use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
};

const ID: &str = "01234567-89ab-cdef-0123-456789abcdef";

async fn task_at(dir: &tempfile::TempDir, size: usize) -> Task {
    let path = dir.path().join("firmware.bin");
    let content: Vec<u8> = (0..size).map(|n| (n % 251) as u8).collect();
    tokio::fs::write(&path, content).await.unwrap();
    Task {
        schema: 1,
        id: ID.into(),
        file_path: path.to_str().unwrap().into(),
        file_info: get_file_info_impl(path.to_str().unwrap()).await.unwrap(),
        target: Target {
            bucket: "test-bucket".into(),
            region: "us-east-1".into(),
            endpoint: None,
            force_path_style: true,
        },
        key: "firmware.bin".into(),
        file_url: "https://example.com/firmware.bin".into(),
        multipart: true,
        checksum: true,
        remote_checksums: true,
        part_size: 5 * 1024 * 1024,
        concurrency: CONCURRENCY,
        upload_id: None,
        parts: BTreeMap::new(),
        phase: "preparing".into(),
        last_error: None,
        etag: None,
        version_id: None,
    }
}
fn store_at(dir: &tempfile::TempDir) -> Store {
    Store {
        root: dir.path().join("tasks"),
    }
}
fn observer() -> Observer {
    Arc::new(|_| {})
}

fn test_settings() -> S3Config {
    S3Config {
        bucket: "test-bucket".into(),
        region: "us-east-1".into(),
        access_key: "test-access".into(),
        secret_key: "test-secret".into(),
        session_token: None,
        endpoint: None,
        force_path_style: true,
        key_prefix: String::new(),
        public_base_url: None,
        connect_ips: Vec::new(),
        stall_grace_seconds: 60,
        part_size: PART_SIZE,
        upload_concurrency: CONCURRENCY,
        send_checksum_headers: true,
    }
}

#[tokio::test]
async fn discard_task_without_upload_id_never_loads_config_and_preserves_source_file() {
    let dir = tempfile::tempdir().unwrap();
    let store = store_at(&dir);
    for multipart in [false, true] {
        for phase in [
            "preparing",
            "failed",
            "paused",
            "completing",
            "verifying",
            "cleanup_pending",
        ] {
            let mut task = task_at(&dir, 10).await;
            task.multipart = multipart;
            task.phase = phase.into();
            store.save(&task).unwrap();
            discard_task(&mut task, &store, || {
                panic!("Local cleanup must not load S3 config")
            })
            .await
            .unwrap();
            assert!(store.list().unwrap().tasks.is_empty());
            assert_eq!(std::fs::metadata(&task.file_path).unwrap().len(), 10);
        }
    }
}

#[tokio::test]
async fn discard_task_with_upload_id_preserves_record_when_config_is_missing_or_changed() {
    let dir = tempfile::tempdir().unwrap();
    let store = store_at(&dir);
    let mut task = task_at(&dir, 10).await;
    task.upload_id = Some("upload-1".into());
    store.save(&task).unwrap();
    let original = serde_json::to_value(&task).unwrap();
    assert!(
        discard_task(&mut task, &store, || Err("缺少存储配置".into()))
            .await
            .unwrap_err()
            .contains("缺少存储配置")
    );
    for field in ["bucket", "region", "endpoint", "force_path_style"] {
        let mut settings = test_settings();
        match field {
            "bucket" => settings.bucket = "other-bucket".into(),
            "region" => settings.region = "other-region".into(),
            "endpoint" => settings.endpoint = Some("https://other.example.com".into()),
            "force_path_style" => settings.force_path_style = false,
            _ => unreachable!(),
        }
        let error = discard_task(&mut task, &store, || Ok(settings))
            .await
            .unwrap_err();
        for expected in [
            "S3_BUCKET=test-bucket",
            "S3_REGION=us-east-1",
            "S3_ENDPOINT=",
            "S3_FORCE_PATH_STYLE=true",
            "重新加载配置",
        ] {
            assert!(error.contains(expected), "{error}");
        }
        assert_eq!(serde_json::to_value(&task).unwrap(), original);
        assert_eq!(
            serde_json::to_value(store.load(ID).unwrap()).unwrap(),
            original
        );
    }
}

#[tokio::test]
async fn discard_task_with_matching_config_cleans_remote_upload_and_preserves_committed_object() {
    let server = FakeS3::start(FakeState {
        committed: true,
        ..Default::default()
    })
    .await;
    let dir = tempfile::tempdir().unwrap();
    let store = store_at(&dir);
    let mut task = task_at(&dir, 10).await;
    let mut settings = test_settings();
    settings.endpoint = Some(server.endpoint.clone());
    task.target = Target::from(&settings);
    task.upload_id = Some("upload-1".into());
    store.save(&task).unwrap();
    discard_task(&mut task, &store, || Ok(settings))
        .await
        .unwrap();
    assert!(store.list().unwrap().tasks.is_empty());
    let state = server.state.lock().unwrap();
    assert_eq!(state.aborted, 1);
    assert!(state.committed);
}

#[derive(Default)]
struct FakeState {
    parts: BTreeMap<i32, ReceivedBody>,
    attempts: BTreeMap<i32, usize>,
    task_id: String,
    checksum: String,
    size: u64,
    committed: bool,
    single: bool,
    retry_part: bool,
    lose_complete_response: bool,
    corrupt_checksum: bool,
    omit_checksum: bool,
    corrupt_download: bool,
    wrong_download_identity: bool,
    downloads: usize,
    reject_checksum: bool,
    fail_abort: bool,
    aborted: usize,
    completed: usize,
    creates: usize,
    max_inflight: usize,
    inflight: usize,
}
struct FakeS3 {
    endpoint: String,
    state: Arc<Mutex<FakeState>>,
    worker: tokio::task::JoinHandle<()>,
}
impl Drop for FakeS3 {
    fn drop(&mut self) {
        self.worker.abort();
    }
}
struct Request {
    method: String,
    url: url::Url,
    headers: BTreeMap<String, String>,
    body: ReceivedBody,
}
struct ReceivedBody {
    bytes: Vec<u8>,
    size: u64,
    sha256: String,
}
impl From<Vec<u8>> for ReceivedBody {
    fn from(bytes: Vec<u8>) -> Self {
        Self {
            size: bytes.len() as u64,
            sha256: STANDARD.encode(Sha256::digest(&bytes)),
            bytes,
        }
    }
}
struct Response {
    code: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    head: bool,
}
impl Response {
    fn xml(code: u16, body: String) -> Self {
        Self {
            code,
            headers: vec![("Content-Type".into(), "application/xml".into())],
            body: body.into_bytes(),
            head: false,
        }
    }
    fn error(code: u16, kind: &str) -> Self {
        Self::xml(
            code,
            format!("<Error><Code>{kind}</Code><Message>mock error</Message></Error>"),
        )
    }
}

async fn read_request(socket: &mut TcpStream, source: Option<&Task>) -> Option<Request> {
    let mut data = Vec::new();
    let header_end = loop {
        let mut buffer = [0; 8192];
        let n = socket.read(&mut buffer).await.ok()?;
        if n == 0 {
            return None;
        }
        data.extend_from_slice(&buffer[..n]);
        if let Some(end) = data.windows(4).position(|p| p == b"\r\n\r\n") {
            break end + 4;
        }
    };
    let header = String::from_utf8(data[..header_end].to_vec()).unwrap();
    let mut lines = header.lines();
    let line: Vec<&str> = lines.next().unwrap().split_whitespace().collect();
    let headers: BTreeMap<String, String> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.to_ascii_lowercase(), v.trim().into()))
        .collect();
    assert!(
        !headers.contains_key("transfer-encoding"),
        "explicit checksums should avoid streaming trailers"
    );
    if headers.get("expect").is_some_and(|v| v == "100-continue") {
        socket
            .write_all(b"HTTP/1.1 100 Continue\r\n\r\n")
            .await
            .ok()?;
    }
    let size = headers
        .get("content-length")
        .map(|v| v.parse::<u64>().unwrap())
        .unwrap_or(0);
    let url = url::Url::parse(&format!("http://localhost{}", line[1])).unwrap();
    // Large-file tests compare every received byte against the intended file
    // range, retaining only size/checksum rather than buffering GiBs in the mock.
    let mut expected_file = if let Some(task) = source.filter(|_| line[0] == "PUT") {
        let (offset, expected_size) = match url.query_pairs().find(|(k, _)| k == "partNumber") {
            Some((_, n)) => {
                let number = n.parse().unwrap();
                assert!((1..=task.part_count()).contains(&number));
                task.range(number)
            }
            None => (0, task.file_info.file_size),
        };
        assert_eq!(size, expected_size, "incorrect uploaded range length");
        let mut file = tokio::fs::File::open(&task.file_path).await.unwrap();
        file.seek(std::io::SeekFrom::Start(offset)).await.unwrap();
        Some(file)
    } else {
        None
    };
    let prefix = &data[header_end..];
    assert!(prefix.len() as u64 <= size);
    let mut input = std::io::Cursor::new(prefix).chain(socket.take(size - prefix.len() as u64));
    let mut body = Vec::new();
    let mut hash = Sha256::new();
    let mut received = 0u64;
    let mut buffer = vec![0; 64 * 1024];
    let mut expected = vec![0; buffer.len()];
    while received < size {
        let limit = (size - received).min(buffer.len() as u64) as usize;
        let n = input.read(&mut buffer[..limit]).await.ok()?;
        if n == 0 {
            return None;
        }
        hash.update(&buffer[..n]);
        if let Some(file) = &mut expected_file {
            file.read_exact(&mut expected[..n]).await.unwrap();
            assert!(
                buffer[..n] == expected[..n],
                "uploaded bytes differ at request offset {received}"
            );
        } else {
            body.extend_from_slice(&buffer[..n]);
        }
        received += n as u64;
    }
    Some(Request {
        method: line[0].into(),
        url,
        headers,
        body: ReceivedBody {
            bytes: body,
            size: received,
            sha256: STANDARD.encode(hash.finalize()),
        },
    })
}

fn respond(r: Request, state: &mut FakeState) -> Response {
    let query: BTreeMap<String, String> = r
        .url
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    if r.method == "HEAD" {
        if !state.committed {
            let mut response = Response::error(404, "NotFound");
            response.head = true;
            return response;
        }
        let mut response = Response {
            code: 200,
            headers: vec![
                ("Content-Length".into(), state.size.to_string()),
                ("ETag".into(), "\"final\"".into()),
                ("x-amz-meta-otacast-task".into(), state.task_id.clone()),
                ("x-amz-version-id".into(), "v1".into()),
                (
                    "x-amz-checksum-type".into(),
                    if state.single {
                        "FULL_OBJECT"
                    } else {
                        "COMPOSITE"
                    }
                    .into(),
                ),
                (
                    "x-amz-checksum-sha256".into(),
                    if state.corrupt_checksum {
                        "wrong".into()
                    } else {
                        state.checksum.clone()
                    },
                ),
            ],
            body: Vec::new(),
            head: true,
        };
        if state.omit_checksum {
            response
                .headers
                .retain(|(name, _)| !name.starts_with("x-amz-checksum-"));
        }
        return response;
    }
    if r.method == "POST" && query.contains_key("uploads") {
        assert_eq!(
            r.headers.get("x-amz-acl").map(String::as_str),
            Some("public-read")
        );
        if state.reject_checksum && r.headers.contains_key("x-amz-checksum-algorithm") {
            return Response::error(400, "NotImplemented");
        }
        state.creates += 1;
        state.task_id = r.headers.get("x-amz-meta-otacast-task").unwrap().clone();
        return Response::xml(200, "<InitiateMultipartUploadResult><Bucket>test-bucket</Bucket><Key>firmware.bin</Key><UploadId>upload-1</UploadId></InitiateMultipartUploadResult>".into());
    }
    if r.method == "PUT" && query.contains_key("partNumber") {
        if state.aborted > 0 || state.committed {
            return Response::error(404, "NoSuchUpload");
        }
        let n = query["partNumber"].parse::<i32>().unwrap();
        let attempt = state.attempts.entry(n).or_default();
        *attempt += 1;
        if state.retry_part && n == 2 && *attempt == 1 {
            return Response::error(500, "InternalError");
        }
        let digest = r.body.sha256.clone();
        if let Some(expected) = r.headers.get("x-amz-checksum-sha256") {
            assert_eq!(expected, &digest, "part range must match checksum");
        }
        state.parts.insert(n, r.body);
        let mut response = Response::xml(200, String::new());
        response
            .headers
            .push(("ETag".into(), format!("\"part-{n}\"")));
        if !state.omit_checksum {
            response
                .headers
                .push(("x-amz-checksum-sha256".into(), digest));
        }
        return response;
    }
    if r.method == "GET" && query.contains_key("uploadId") {
        if state.committed || state.aborted > 0 {
            return Response::error(404, "NoSuchUpload");
        }
        let parts: String = state.parts.iter().map(|(n,b)| {
            let checksum = if state.omit_checksum { String::new() } else { format!("<ChecksumSHA256>{}</ChecksumSHA256>", b.sha256) };
            format!("<Part><PartNumber>{n}</PartNumber><ETag>\"part-{n}\"</ETag><Size>{}</Size>{checksum}</Part>", b.size)
        }).collect();
        return Response::xml(200, format!("<ListPartsResult><Bucket>test-bucket</Bucket><Key>firmware.bin</Key><UploadId>upload-1</UploadId><IsTruncated>false</IsTruncated>{parts}</ListPartsResult>"));
    }
    if r.method == "POST" && query.contains_key("uploadId") {
        state.completed += 1;
        if state.committed {
            return Response::error(404, "NoSuchUpload");
        }
        let xml = String::from_utf8(r.body.bytes).unwrap();
        let mut previous = 0;
        for n in state.parts.keys() {
            let position = xml.find(&format!("<PartNumber>{n}</PartNumber>")).unwrap();
            assert!(position >= previous);
            previous = position;
        }
        let mut composite = Sha256::new();
        for part in state.parts.values() {
            composite.update(STANDARD.decode(&part.sha256).unwrap());
        }
        state.checksum = format!(
            "{}-{}",
            STANDARD.encode(composite.finalize()),
            state.parts.len()
        );
        if let Some(expected) = r.headers.get("x-amz-checksum-sha256") {
            assert_eq!(expected, &state.checksum);
        }
        state.size = state.parts.values().map(|b| b.size).sum();
        state.committed = true;
        if state.lose_complete_response {
            return Response::error(500, "InternalError");
        }
        let mut response = Response::xml(
            200,
            "<CompleteMultipartUploadResult><ETag>\"final\"</ETag></CompleteMultipartUploadResult>"
                .into(),
        );
        response
            .headers
            .push(("x-amz-version-id".into(), "v1".into()));
        return response;
    }
    if r.method == "DELETE" {
        if state.fail_abort {
            return Response::error(403, "AccessDenied");
        }
        state.aborted += 1;
        state.parts.clear();
        return Response::xml(204, String::new());
    }
    if r.method == "PUT" {
        assert_eq!(
            r.headers.get("x-amz-acl").map(String::as_str),
            Some("public-read")
        );
        state.single = true;
        state.committed = true;
        state.task_id = r.headers.get("x-amz-meta-otacast-task").unwrap().clone();
        state.size = r.body.size;
        state.checksum = r.body.sha256.clone();
        state.parts.insert(1, r.body);
        if let Some(expected) = r.headers.get("x-amz-checksum-sha256") {
            assert_eq!(expected, &state.checksum);
        }
        let mut response = Response::xml(200, String::new());
        response.headers.push(("ETag".into(), "\"final\"".into()));
        return response;
    }
    if r.method == "GET" {
        if !state.committed {
            return Response::error(404, "NotFound");
        }
        assert_eq!(
            r.headers.get("if-match").map(String::as_str),
            Some("\"final\"")
        );
        state.downloads += 1;
        let mut body: Vec<u8> = state
            .parts
            .values()
            .flat_map(|part| &part.bytes)
            .copied()
            .collect();
        if state.corrupt_download && !body.is_empty() {
            body[0] ^= 1;
        }
        return Response {
            code: 200,
            headers: vec![
                ("Content-Length".into(), state.size.to_string()),
                ("ETag".into(), "\"final\"".into()),
                ("x-amz-version-id".into(), "v1".into()),
                (
                    "x-amz-meta-otacast-task".into(),
                    if state.wrong_download_identity {
                        "other-task".into()
                    } else {
                        state.task_id.clone()
                    },
                ),
            ],
            body,
            head: false,
        };
    }
    panic!("unexpected request {} {}", r.method, r.url);
}

impl FakeS3 {
    async fn start(state: FakeState) -> Self {
        Self::start_with_source(state, None).await
    }
    async fn start_with_source(state: FakeState, source: Option<Task>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(Mutex::new(state));
        let shared = state.clone();
        let worker = tokio::spawn(async move {
            let mut handlers = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (mut socket, _) = accepted.unwrap(); let state = shared.clone(); let source = source.clone();
                        handlers.spawn(async move {
                            let Some(r) = read_request(&mut socket, source.as_ref()).await else { return; };
                            let part = r.method == "PUT" && r.url.query_pairs().any(|(k,_)| k == "partNumber");
                            if part {
                                { let mut s = state.lock().unwrap(); s.inflight += 1; s.max_inflight = s.max_inflight.max(s.inflight); }
                                tokio::time::sleep(Duration::from_millis(20)).await;
                            }
                            let response = { let mut s = state.lock().unwrap(); if part { s.inflight -= 1; } respond(r, &mut s) };
                            let mut header = format!("HTTP/1.1 {} OK\r\nConnection: close\r\n", response.code);
                            if !response.headers.iter().any(|(k,_)| k == "Content-Length") { header.push_str(&format!("Content-Length: {}\r\n", response.body.len())); }
                            for (key, value) in response.headers { header.push_str(&format!("{key}: {value}\r\n")); }
                            header.push_str("\r\n");
                            if socket.write_all(header.as_bytes()).await.is_ok() && !response.head { let _ = socket.write_all(&response.body).await; }
                        });
                    },
                    result = handlers.join_next(), if !handlers.is_empty() => { result.unwrap().unwrap(); },
                }
            }
        });
        Self {
            endpoint,
            state,
            worker,
        }
    }
    fn client(&self) -> Client {
        let mut settings = test_settings();
        settings.endpoint = Some(self.endpoint.clone());
        client(&settings)
    }
}

#[tokio::test]
async fn multipart_retries_only_failed_part_and_checks_remote_content() {
    let server = FakeS3::start(FakeState {
        retry_part: true,
        ..Default::default()
    })
    .await;
    let dir = tempfile::tempdir().unwrap();
    let task = task_at(&dir, 11 * 1024 * 1024).await;
    let store = store_at(&dir);
    store.save(&task).unwrap();
    let manager = UploadManager::default();
    let lease = manager.acquire(ID).unwrap();
    let outcome = run(
        server.client(),
        task.clone(),
        store.clone(),
        observer(),
        &lease,
    )
    .await
    .unwrap();
    assert_eq!(outcome.result.unwrap().verification, "sha256_composite");
    assert!(store.list().unwrap().tasks.is_empty());
    let s = server.state.lock().unwrap();
    assert_eq!(s.attempts, BTreeMap::from([(1, 1), (2, 2), (3, 1)]));
    assert_eq!(s.size, task.file_info.file_size);
    assert_eq!(s.creates, 1);
    assert_eq!(s.completed, 1);
    assert!(s.max_inflight <= CONCURRENCY);
    let bytes = std::fs::read(&task.file_path).unwrap();
    assert_eq!(
        s.parts
            .values()
            .flat_map(|part| &part.bytes)
            .copied()
            .collect::<Vec<_>>(),
        bytes
    );
}

#[tokio::test]
async fn pause_reload_and_resume_preserves_confirmed_parts() {
    let server = FakeS3::start(FakeState::default()).await;
    let dir = tempfile::tempdir().unwrap();
    let task = task_at(&dir, 11 * 1024 * 1024).await;
    let store = store_at(&dir);
    store.save(&task).unwrap();
    let manager = Arc::new(UploadManager::default());
    let lease = manager.acquire(ID).unwrap();
    let requested = Arc::new(AtomicBool::new(false));
    let observe: Observer = {
        let manager = manager.clone();
        let store = store.clone();
        let requested = requested.clone();
        Arc::new(move |p| {
            if p.phase == "uploading"
                && store.load(ID).is_ok_and(|t| !t.parts.is_empty())
                && !requested.swap(true, Ordering::SeqCst)
            {
                manager.signal(ID, Control::Pause).unwrap();
            }
        })
    };
    assert_eq!(
        run(server.client(), task, store.clone(), observe, &lease)
            .await
            .unwrap()
            .status,
        "paused"
    );
    drop(lease);
    let saved = store.load(ID).unwrap();
    assert!(!saved.parts.is_empty());
    let attempts = server.state.lock().unwrap().attempts.clone();
    let confirmed: Vec<_> = saved.parts.keys().copied().collect();
    let new_manager = UploadManager::default();
    let lease = new_manager.acquire(ID).unwrap();
    let outcome = run(server.client(), saved, store.clone(), observer(), &lease)
        .await
        .unwrap();
    assert_eq!(outcome.status, "complete");
    let s = server.state.lock().unwrap();
    for n in confirmed {
        assert_eq!(
            s.attempts[&n], attempts[&n],
            "confirmed part {n} was uploaded again"
        );
    }
    assert_eq!(s.creates, 1);
}

#[tokio::test]
async fn lost_completion_response_is_reconciled_without_reupload() {
    let server = FakeS3::start(FakeState {
        lose_complete_response: true,
        ..Default::default()
    })
    .await;
    let dir = tempfile::tempdir().unwrap();
    let task = task_at(&dir, 1024).await;
    let store = store_at(&dir);
    store.save(&task).unwrap();
    let manager = UploadManager::default();
    let lease = manager.acquire(ID).unwrap();
    assert_eq!(
        run(server.client(), task, store, observer(), &lease)
            .await
            .unwrap()
            .status,
        "complete"
    );
    let s = server.state.lock().unwrap();
    assert_eq!(s.attempts[&1], 1);
    assert_eq!(s.completed, 2);
}

#[tokio::test]
async fn remote_checksum_mismatch_keeps_journal_and_does_not_reupload_on_resume() {
    let server = FakeS3::start(FakeState {
        corrupt_checksum: true,
        ..Default::default()
    })
    .await;
    let dir = tempfile::tempdir().unwrap();
    let task = task_at(&dir, 1024).await;
    let store = store_at(&dir);
    store.save(&task).unwrap();
    let manager = UploadManager::default();
    let lease = manager.acquire(ID).unwrap();
    assert!(
        run(server.client(), task, store.clone(), observer(), &lease)
            .await
            .unwrap_err()
            .contains("SHA-256")
    );
    drop(lease);
    let saved = store.load(ID).unwrap();
    assert_eq!(saved.phase, "verifying");
    let lease = manager.acquire(ID).unwrap();
    assert!(
        run(server.client(), saved, store.clone(), observer(), &lease)
            .await
            .is_err()
    );
    assert_eq!(server.state.lock().unwrap().attempts[&1], 1);
}

#[tokio::test]
async fn cancel_cleanup_is_retryable_and_never_deletes_committed_objects() {
    let server = FakeS3::start(FakeState {
        fail_abort: true,
        ..Default::default()
    })
    .await;
    let dir = tempfile::tempdir().unwrap();
    let mut task = task_at(&dir, 1024).await;
    task.upload_id = Some("upload-1".into());
    let store = store_at(&dir);
    store.save(&task).unwrap();
    let manager = UploadManager::default();
    let lease = manager.acquire(ID).unwrap();
    manager.signal(ID, Control::Cancel).unwrap();
    assert!(
        run(server.client(), task, store.clone(), observer(), &lease)
            .await
            .unwrap_err()
            .contains("AccessDenied")
    );
    let saved = store.load(ID).unwrap();
    assert_eq!(saved.phase, "cleanup_pending");
    {
        let mut s = server.state.lock().unwrap();
        s.fail_abort = false;
        s.committed = true;
    }
    cleanup(&server.client(), &saved, &store).await.unwrap();
    assert!(store.list().unwrap().tasks.is_empty());
    assert!(server.state.lock().unwrap().committed);
}

#[tokio::test]
async fn single_empty_file_and_explicit_size_only_mode() {
    for size in [0, 1000] {
        let server = FakeS3::start(FakeState {
            corrupt_checksum: true,
            ..Default::default()
        })
        .await;
        let dir = tempfile::tempdir().unwrap();
        let mut task = task_at(&dir, size).await;
        task.multipart = false;
        task.checksum = false;
        let store = store_at(&dir);
        store.save(&task).unwrap();
        let manager = UploadManager::default();
        let lease = manager.acquire(ID).unwrap();
        let outcome = run(server.client(), task, store, observer(), &lease)
            .await
            .unwrap();
        assert_eq!(outcome.result.unwrap().verification, "size_only");
    }
}

#[tokio::test]
async fn changed_local_file_is_rejected_before_remote_upload() {
    let server = FakeS3::start(FakeState::default()).await;
    let dir = tempfile::tempdir().unwrap();
    let task = task_at(&dir, 1024).await;
    let store = store_at(&dir);
    store.save(&task).unwrap();
    std::fs::write(&task.file_path, b"changed").unwrap();
    let manager = UploadManager::default();
    let lease = manager.acquire(ID).unwrap();
    assert!(
        run(server.client(), task, store.clone(), observer(), &lease)
            .await
            .unwrap_err()
            .contains("本地文件已变化")
    );
    assert_eq!(server.state.lock().unwrap().creates, 0);
    assert!(store.load(ID).is_ok());
}

#[tokio::test]
async fn unsupported_checksum_never_silently_downgrades() {
    let server = FakeS3::start(FakeState {
        reject_checksum: true,
        ..Default::default()
    })
    .await;
    let dir = tempfile::tempdir().unwrap();
    let task = task_at(&dir, 1024).await;
    let store = store_at(&dir);
    store.save(&task).unwrap();
    let manager = UploadManager::default();
    let lease = manager.acquire(ID).unwrap();
    assert!(
        run(server.client(), task, store.clone(), observer(), &lease)
            .await
            .is_err()
    );
    assert!(store.load(ID).unwrap().checksum);
    assert!(!server.state.lock().unwrap().committed);
}

#[tokio::test]
async fn journal_ranges_support_six_gib_and_reject_invalid_records() {
    let dir = tempfile::tempdir().unwrap();
    let mut task = task_at(&dir, 0).await;
    task.file_info.file_size = 6 * 1024 * 1024 * 1024;
    task.part_size = PART_SIZE;
    assert_eq!(task.part_count(), 96);
    assert_eq!(task.range(96), (95 * PART_SIZE, PART_SIZE));
    task.file_info.file_size += 1;
    assert_eq!(task.part_count(), 97);
    assert_eq!(task.range(97).1, 1);
    let store = store_at(&dir);
    store.save(&task).unwrap();
    task.phase = "paused".into();
    store.save(&task).unwrap();
    assert_eq!(store.load(ID).unwrap().phase, "paused");
    task.parts.insert(
        0,
        Part {
            number: 0,
            size: 0,
            sha256: String::new(),
            etag: String::new(),
        },
    );
    assert!(store.save(&task).is_err());
    assert!(store.load("../../outside").is_err());
    let json = std::fs::read_to_string(store.root.join(format!("{ID}.json"))).unwrap();
    assert!(!json.contains("test-secret"));
    assert!(!json.contains("access_key"));
}

#[tokio::test]
async fn progress_retry_does_not_double_count_and_commit_boundary_rejects_cancel() {
    let dir = tempfile::tempdir().unwrap();
    let task = task_at(&dir, 100).await;
    let events = Arc::new(Mutex::new(Vec::new()));
    let observed = events.clone();
    let p = ProgressTracker::new(&task, Arc::new(move |p| observed.lock().unwrap().push(p)));
    p.sent(1, 50);
    p.sent(2, 20);
    p.sent(1, 0);
    p.sent(1, 50);
    p.confirm(&Part {
        number: 1,
        size: 50,
        etag: String::new(),
        sha256: String::new(),
    });
    assert_eq!(events.lock().unwrap().last().unwrap().uploaded_bytes, 70);
    let manager = UploadManager::default();
    let lease = manager.acquire(ID).unwrap();
    assert!(manager.acquire(ID).is_err());
    manager.seal().unwrap();
    assert!(manager.signal(ID, Control::Cancel).is_err());
    drop(lease);
    assert!(manager.acquire(ID).is_ok());
    let _writer = writer_lock(&store_at(&dir)).unwrap();
    assert!(writer_lock(&store_at(&dir)).is_err());
}

#[tokio::test]
async fn single_put_validates_full_sha256_for_empty_and_nonempty_files() {
    for size in [0, 1000] {
        let server = FakeS3::start(FakeState::default()).await;
        let dir = tempfile::tempdir().unwrap();
        let mut task = task_at(&dir, size).await;
        task.multipart = false;
        let store = store_at(&dir);
        store.save(&task).unwrap();
        let manager = UploadManager::default();
        let lease = manager.acquire(ID).unwrap();
        let outcome = run(server.client(), task, store, observer(), &lease)
            .await
            .unwrap();
        assert_eq!(outcome.result.unwrap().verification, "sha256");
    }
}

#[tokio::test]
async fn resume_detects_content_change_even_when_size_and_mtime_are_preserved() {
    let server = FakeS3::start(FakeState::default()).await;
    let dir = tempfile::tempdir().unwrap();
    let mut task = task_at(&dir, 1024).await;
    let bytes = std::fs::read(&task.file_path).unwrap();
    let original_time = std::fs::metadata(&task.file_path)
        .unwrap()
        .modified()
        .unwrap();
    task.upload_id = Some("upload-1".into());
    task.parts.insert(
        1,
        Part {
            number: 1,
            size: 1024,
            etag: "\"part-1\"".into(),
            sha256: STANDARD.encode(Sha256::digest(&bytes)),
        },
    );
    server
        .state
        .lock()
        .unwrap()
        .parts
        .insert(1, bytes.clone().into());
    let mut modified = bytes;
    modified[0] ^= 1;
    std::fs::write(&task.file_path, modified).unwrap();
    std::fs::File::options()
        .write(true)
        .open(&task.file_path)
        .unwrap()
        .set_modified(original_time)
        .unwrap();
    unchanged(&task).await.unwrap();
    let store = store_at(&dir);
    store.save(&task).unwrap();
    let manager = UploadManager::default();
    let lease = manager.acquire(ID).unwrap();
    assert!(run(server.client(), task, store, observer(), &lease)
        .await
        .unwrap_err()
        .contains("本地内容发生变化"));
    assert!(server.state.lock().unwrap().attempts.is_empty());
}

#[tokio::test]
async fn active_cancellation_drains_workers_and_removes_remote_upload() {
    let server = FakeS3::start(FakeState::default()).await;
    let dir = tempfile::tempdir().unwrap();
    let task = task_at(&dir, 11 * 1024 * 1024).await;
    let store = store_at(&dir);
    store.save(&task).unwrap();
    let manager = Arc::new(UploadManager::default());
    let lease = manager.acquire(ID).unwrap();
    let requested = Arc::new(AtomicBool::new(false));
    let observe: Observer = {
        let manager = manager.clone();
        let store = store.clone();
        Arc::new(move |p| {
            if p.phase == "uploading"
                && store.load(ID).is_ok_and(|t| !t.parts.is_empty())
                && !requested.swap(true, Ordering::SeqCst)
            {
                manager.signal(ID, Control::Cancel).unwrap();
            }
        })
    };
    assert_eq!(
        run(server.client(), task, store.clone(), observe, &lease)
            .await
            .unwrap()
            .status,
        "cancelled"
    );
    assert!(store.list().unwrap().tasks.is_empty());
    let s = server.state.lock().unwrap();
    assert!(s.aborted > 0);
    assert!(!s.committed);
    assert!(s.parts.is_empty());
}

#[tokio::test]
async fn corrupt_journal_does_not_hide_valid_recovery_tasks() {
    let dir = tempfile::tempdir().unwrap();
    let task = task_at(&dir, 0).await;
    let store = store_at(&dir);
    store.save(&task).unwrap();
    std::fs::write(
        store.root.join("11111111-1111-1111-1111-111111111111.json"),
        b"broken",
    )
    .unwrap();
    let listing = store.list().unwrap();
    assert_eq!(listing.tasks.len(), 1);
    assert_eq!(listing.warnings.len(), 1);
    assert!(store
        .root
        .join("11111111-1111-1111-1111-111111111111.json")
        .exists());
}

#[path = "upload_size_tests.rs"]
mod sizes;

#[path = "upload_live_tests.rs"]
mod live;

#[tokio::test]
async fn missing_remote_checksum_downloads_and_verifies_empty_single_and_multipart_content() {
    for (size, multipart) in [(0, false), (1000, false), (11 * 1024 * 1024, true)] {
        let server = FakeS3::start(FakeState {
            omit_checksum: true,
            ..Default::default()
        })
        .await;
        let dir = tempfile::tempdir().unwrap();
        let mut task = task_at(&dir, size).await;
        task.multipart = multipart;
        let store = store_at(&dir);
        store.save(&task).unwrap();
        let manager = UploadManager::default();
        let lease = manager.acquire(ID).unwrap();
        let result = run(server.client(), task, store.clone(), observer(), &lease)
            .await
            .unwrap();
        assert_eq!(result.result.unwrap().verification, "download_sha256");
        assert_eq!(server.state.lock().unwrap().downloads, 1);
        assert!(store.list().unwrap().tasks.is_empty());
    }
}

#[tokio::test]
async fn download_verification_rejects_corrupt_content_and_wrong_object_identity() {
    for corrupt in [true, false] {
        let server = FakeS3::start(FakeState {
            omit_checksum: true,
            corrupt_download: corrupt,
            wrong_download_identity: !corrupt,
            ..Default::default()
        })
        .await;
        let dir = tempfile::tempdir().unwrap();
        let mut task = task_at(&dir, 1024).await;
        task.multipart = false;
        let store = store_at(&dir);
        store.save(&task).unwrap();
        let manager = UploadManager::default();
        let lease = manager.acquire(ID).unwrap();
        let error = run(server.client(), task, store.clone(), observer(), &lease)
            .await
            .unwrap_err();
        assert!(error.contains(if corrupt {
            "下载内容 SHA-256 校验失败"
        } else {
            "对象标识"
        }));
        assert_eq!(store.load(ID).unwrap().phase, "verifying");
        assert!(server.state.lock().unwrap().committed);
    }
}

#[tokio::test]
async fn resume_retains_confirmed_parts_when_service_omits_part_checksums() {
    let server = FakeS3::start(FakeState {
        omit_checksum: true,
        ..Default::default()
    })
    .await;
    let dir = tempfile::tempdir().unwrap();
    let mut task = task_at(&dir, 1024).await;
    let bytes = std::fs::read(&task.file_path).unwrap();
    task.upload_id = Some("upload-1".into());
    task.parts.insert(
        1,
        Part {
            number: 1,
            size: 1024,
            etag: "\"part-1\"".into(),
            sha256: STANDARD.encode(Sha256::digest(&bytes)),
        },
    );
    {
        let mut state = server.state.lock().unwrap();
        state.task_id = task.id.clone();
        state.parts.insert(1, bytes.into());
    }
    let store = store_at(&dir);
    store.save(&task).unwrap();
    let manager = UploadManager::default();
    let lease = manager.acquire(ID).unwrap();
    let result = run(
        server.client(),
        store.load(ID).unwrap(),
        store,
        observer(),
        &lease,
    )
    .await
    .unwrap();
    assert_eq!(result.result.unwrap().verification, "download_sha256");
    assert!(server.state.lock().unwrap().attempts.is_empty());
}

#[tokio::test]
async fn configured_small_parts_and_serial_upload_preserve_content_and_legacy_journals() {
    let server = FakeS3::start(FakeState::default()).await;
    let dir = tempfile::tempdir().unwrap();
    let mut task = task_at(&dir, 11 * 1024 * 1024).await;
    let mut legacy = serde_json::to_value(&task).unwrap();
    legacy.as_object_mut().unwrap().remove("concurrency");
    let restored: Task = serde_json::from_value(legacy).unwrap();
    assert_eq!(restored.concurrency, 3);
    task.concurrency = 0;
    assert!(task.validate().is_err());
    task.concurrency = 1;
    task.part_size = 8 * 1024 * 1024;
    let original = std::fs::read(&task.file_path).unwrap();
    let store = store_at(&dir);
    store.save(&task).unwrap();
    let manager = UploadManager::default();
    let lease = manager.acquire(ID).unwrap();
    assert_eq!(
        run(server.client(), task, store, observer(), &lease)
            .await
            .unwrap()
            .status,
        "complete"
    );
    let state = server.state.lock().unwrap();
    assert_eq!(state.max_inflight, 1);
    assert_eq!(state.parts[&1].size, 8 * 1024 * 1024);
    assert_eq!(state.parts[&2].size, 3 * 1024 * 1024);
    assert_eq!(
        state
            .parts
            .values()
            .flat_map(|part| &part.bytes)
            .copied()
            .collect::<Vec<_>>(),
        original
    );
}

#[test]
fn transport_errors_explain_stalls_and_resets_without_persisting_sensitive_sources() {
    use aws_smithy_runtime_api::client::result::ConnectorError;
    for (source, expected) in [
        ("minimum throughput was specified at 1 B/s, but throughput of 0 B/s was observed secret-value", "停滞保护"),
        ("Connection reset by peer secret-value", "对端重置"),
    ] {
        let error: aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::put_object::PutObjectError> = aws_sdk_s3::error::SdkError::dispatch_failure(ConnectorError::io(std::io::Error::other(source).into()));
        let Failure::Error(message) = aws_error("上传失败", error) else { panic!("expected an error"); };
        assert!(message.contains(expected));
        assert!(!message.contains("secret-value"));
    }
}
