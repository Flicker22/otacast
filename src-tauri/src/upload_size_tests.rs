use super::*;
use std::time::Instant;

const MIB: u64 = 1024 * 1024;
const GIB: u64 = 1024 * MIB;

pub(super) async fn sized_task(dir: &tempfile::TempDir, size: u64) -> (Task, String) {
    let mut task = task_at(dir, 0).await;
    let mut file = tokio::fs::File::create(&task.file_path).await.unwrap();
    let mut buffer: Vec<u8> = (0..MIB).map(|n| (n % 251) as u8).collect();
    let mut md5 = crate::Md5::new();
    let mut written = 0;
    while written < size {
        // Each MiB has its own marker, including beyond the 32-bit boundary.
        // Repeated or reordered ranges therefore cannot pass the byte check.
        buffer[..8].copy_from_slice(&written.to_le_bytes());
        let count = (size - written).min(MIB) as usize;
        file.write_all(&buffer[..count]).await.unwrap();
        md5.update(&buffer[..count]);
        written += count as u64;
    }
    file.flush().await.unwrap();
    drop(file);
    task.file_info = get_file_info_impl(&task.file_path).await.unwrap();
    task.multipart = size > MULTIPART_THRESHOLD;
    task.part_size = PART_SIZE;
    (task, hex::encode(md5.finalize()))
}

async fn transfer_file(size: u64, retry: bool, pause: bool) {
    let dir = tempfile::tempdir().unwrap();
    let (task, expected_md5) = sized_task(&dir, size).await;
    let server = FakeS3::start_with_source(
        FakeState {
            retry_part: retry,
            ..Default::default()
        },
        Some(task.clone()),
    )
    .await;
    let store = store_at(&dir);
    store.save(&task).unwrap();
    let manager = Arc::new(UploadManager::default());
    let lease = manager.acquire(ID).unwrap();
    let last_progress = Arc::new(Mutex::new(None));
    let paused = Arc::new(AtomicBool::new(false));
    let observe: Observer = {
        let last = last_progress.clone();
        let paused = paused.clone();
        let manager = manager.clone();
        let store = store.clone();
        Arc::new(move |p| {
            assert_eq!(p.total_bytes, size);
            assert!(p.uploaded_bytes <= size);
            if pause
                && p.phase == "uploading"
                && store.load(ID).is_ok_and(|t| !t.parts.is_empty())
                && !paused.swap(true, Ordering::SeqCst)
            {
                manager.signal(ID, Control::Pause).unwrap();
            }
            *last.lock().unwrap() = Some(p);
        })
    };
    let started = Instant::now();
    let mut outcome = run(
        server.client(),
        task.clone(),
        store.clone(),
        observe.clone(),
        &lease,
    )
    .await
    .unwrap();
    drop(lease);
    let mut confirmed_attempts = BTreeMap::new();
    if pause {
        assert_eq!(outcome.status, "paused");
        let saved = store.load(ID).unwrap();
        assert!(!saved.parts.is_empty());
        let attempts = server.state.lock().unwrap().attempts.clone();
        for number in saved.parts.keys() {
            confirmed_attempts.insert(*number, attempts[number]);
        }
        // A new manager and a journal loaded from disk model restarting the app.
        let resumed_manager = UploadManager::default();
        let resumed_lease = resumed_manager.acquire(ID).unwrap();
        outcome = run(
            server.client(),
            saved,
            store.clone(),
            observe,
            &resumed_lease,
        )
        .await
        .unwrap();
    }
    let upload_seconds = started.elapsed().as_secs_f64();
    assert_eq!(outcome.status, "complete");
    let result = outcome.result.unwrap();
    assert_eq!(result.file_info.file_size, size);
    assert_eq!(
        result.verification,
        if task.multipart {
            "sha256_composite"
        } else {
            "sha256"
        }
    );
    assert!(store.list().unwrap().tasks.is_empty());
    {
        let last = last_progress.lock().unwrap();
        let last = last.as_ref().expect("missing progress events");
        assert_eq!(last.phase, "complete");
        assert_eq!(last.uploaded_bytes, size);
    }
    {
        let state = server.state.lock().unwrap();
        assert!(state.committed);
        assert_eq!(state.size, size);
        assert_eq!(state.single, !task.multipart);
        assert_eq!(state.creates, usize::from(task.multipart));
        assert_eq!(state.completed, usize::from(task.multipart));
        if task.multipart {
            assert_eq!(state.parts.len(), task.part_count() as usize);
            assert_eq!(state.attempts.len(), task.part_count() as usize);
            for (number, part) in &state.parts {
                assert_eq!(part.size, task.range(*number).1);
                assert!(part.bytes.is_empty(), "the mock must stream large bodies");
            }
            if retry {
                assert!(state.attempts[&2] >= 2);
            } else if !pause {
                assert!(state.attempts.values().all(|attempts| *attempts == 1));
            }
            for (number, attempts) in confirmed_attempts {
                assert_eq!(
                    state.attempts[&number], attempts,
                    "confirmed part retransmitted"
                );
            }
        }
    }
    let md5_started = Instant::now();
    let md5 = crate::calculate_md5_impl(
        &task.file_path,
        size,
        &task.file_info.modified_at,
        &result.file_url,
    )
    .unwrap();
    assert_eq!(md5.md5_hash, expected_md5);
    assert_eq!(
        md5.md5_url,
        format!("{}?md5={expected_md5}", result.file_url)
    );
    println!(
        "size={size} bytes mode={} parts={} retry={retry} pause_resume={pause} upload={upload_seconds:.2}s md5={:.2}s verification={} content=byte_matched",
        if task.multipart { "multipart" } else { "single_put" },
        task.part_count(),
        md5_started.elapsed().as_secs_f64(),
        result.verification,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn small_files_and_multipart_threshold_transfer_every_byte() {
    tokio::time::timeout(Duration::from_secs(180), async {
        for size in [0, MIB, MULTIPART_THRESHOLD, MULTIPART_THRESHOLD + 1] {
            transfer_file(size, false, false).await;
        }
    })
    .await
    .expect("small-file and multipart-threshold transfer timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "transfers 12 GiB over loopback; run explicitly with --ignored --nocapture"]
async fn gibibyte_files_transfer_retry_and_resume() {
    tokio::time::timeout(Duration::from_secs(1200), async {
        transfer_file(2 * GIB, false, false).await;
        transfer_file(4 * GIB, true, false).await;
        transfer_file(6 * GIB + 1, false, true).await;
    })
    .await
    .expect("2/4/6-GiB transfer timed out");
}
