#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod env_config;
mod object_url;
mod s3_config;
mod s3_transport;
mod upload;
mod upload_progress;
mod upload_store;

use md5::{Digest, Md5};
use s3_config::{PublicS3Config, S3Config};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct FileInfo {
    file_name: String,
    file_size: u64,
    modified_at: String,
}

#[derive(Debug, Serialize)]
struct UploadResult {
    file_info: FileInfo,
    s3_key: String,
    file_url: String,
    etag: String,
    verification: String,
}

#[derive(Debug, Serialize)]
struct Md5Result {
    md5_hash: String,
    md5_url: String,
}

#[derive(Debug, thiserror::Error)]
enum AppError {
    #[error("IO错误: {0}")]
    Io(#[from] std::io::Error),
    #[error("配置错误: {0}")]
    Config(String),
}

fn file_info(file_path: &Path, metadata: &std::fs::Metadata) -> Result<FileInfo, AppError> {
    if !metadata.is_file() {
        return Err(AppError::Config("请选择一个普通文件".into()));
    }
    Ok(FileInfo {
        file_name: file_path
            .file_name()
            .ok_or_else(|| AppError::Config("无效的文件路径".into()))?
            .to_string_lossy()
            .to_string(),
        file_size: metadata.len(),
        modified_at: metadata
            .modified()?
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| AppError::Config("文件修改时间无效".into()))?
            .as_nanos()
            .to_string(),
    })
}

async fn get_file_info_impl(file_path: &str) -> Result<FileInfo, AppError> {
    file_info(Path::new(file_path), &tokio::fs::metadata(file_path).await?)
}

#[tauri::command]
async fn get_file_info(file_path: String) -> Result<FileInfo, String> {
    get_file_info_impl(&file_path)
        .await
        .map_err(|error| error.to_string())
}

#[tauri::command]
fn get_s3_config() -> Result<PublicS3Config, String> {
    S3Config::load().map(PublicS3Config::from)
}

fn calculate_md5_impl(
    file_path: &str,
    expected_size: u64,
    expected_modified: &str,
    file_url: &str,
) -> Result<Md5Result, AppError> {
    let file = File::open(file_path)?;
    let before = file_info(Path::new(file_path), &file.metadata()?)?;
    if before.file_size != expected_size || before.modified_at != expected_modified {
        return Err(AppError::Config(
            "本地文件已变化，请重新上传后再计算 MD5".into(),
        ));
    }
    let mut reader = BufReader::new(file);
    let mut hasher = Md5::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    if file_info(Path::new(file_path), &reader.get_ref().metadata()?)? != before {
        return Err(AppError::Config(
            "文件在计算过程中发生变化，请重新上传".into(),
        ));
    }
    let md5_hash = hex::encode(hasher.finalize());
    let md5_url = object_url::md5_url(file_url, &md5_hash).map_err(AppError::Config)?;
    Ok(Md5Result { md5_hash, md5_url })
}

#[tauri::command]
async fn calculate_md5(
    file_path: String,
    expected_size: u64,
    expected_modified: String,
    file_url: String,
) -> Result<Md5Result, String> {
    tokio::task::spawn_blocking(move || {
        calculate_md5_impl(&file_path, expected_size, &expected_modified, &file_url)
    })
    .await
    .map_err(|_| "MD5 计算任务失败".to_string())?
    .map_err(|error| error.to_string())
}

fn main() {
    tauri::Builder::default()
        .manage(upload::UploadManager::default())
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_opener::init())
        .invoke_handler(tauri::generate_handler![
            get_file_info,
            get_s3_config,
            upload::upload_file_to_s3,
            upload::resume_upload,
            upload::list_upload_tasks,
            upload::control_upload,
            upload::discard_upload,
            calculate_md5
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn selecting_file_only_reads_metadata_and_md5_is_computed_on_demand() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), b"hello").unwrap();
        let path = file.path().to_str().unwrap();
        let info = get_file_info_impl(path).await.unwrap();
        assert_eq!(info.file_size, 5);
        let result = calculate_md5_impl(
            path,
            info.file_size,
            &info.modified_at,
            "https://cdn.example.com/firmware.bin",
        )
        .unwrap();
        assert_eq!(result.md5_hash, "5d41402abc4b2a76b9719d911017c592");
        assert_eq!(
            result.md5_url,
            "https://cdn.example.com/firmware.bin?md5=5d41402abc4b2a76b9719d911017c592"
        );
        std::fs::write(file.path(), b"modified firmware").unwrap();
        assert!(calculate_md5_impl(
            path,
            info.file_size,
            &info.modified_at,
            "https://cdn.example.com/firmware.bin"
        )
        .unwrap_err()
        .to_string()
        .contains("本地文件已变化"));
    }

    #[tokio::test]
    async fn supports_empty_file_md5_and_rejects_directories() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.path().to_str().unwrap();
        let info = get_file_info_impl(path).await.unwrap();
        let result = calculate_md5_impl(
            path,
            0,
            &info.modified_at,
            "https://cdn.example.com/empty.bin",
        )
        .unwrap();
        assert_eq!(result.md5_hash, "d41d8cd98f00b204e9800998ecf8427e");
        let directory = tempfile::tempdir().unwrap();
        assert!(get_file_info_impl(directory.path().to_str().unwrap())
            .await
            .is_err());
    }
}
