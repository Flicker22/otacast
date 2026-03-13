#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::{Client, config::Region};
use base64::{Engine as _, engine::general_purpose};
use md5::{Digest, Md5};
use serde::Serialize;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

#[derive(Debug, Serialize)]
struct FileInfo {
    file_name: String,
    file_size: u64,
    md5_hash: String,
}

#[derive(Debug, Serialize)]
struct UploadResult {
    file_name: String,
    file_size: u64,
    md5_hash: String,
    s3_key: String,
    etag: String,
}

#[derive(Debug, thiserror::Error)]
enum AppError {
    #[error("IO错误: {0}")]
    Io(#[from] std::io::Error),
    #[error("AWS S3错误: {0}")]
    S3(String),
    #[error("配置错误: {0}")]
    Config(String),
}

// 内部实现
async fn get_file_info_impl(file_path: String) -> Result<FileInfo, AppError> {
    let path = Path::new(&file_path);
    let file_name = path
        .file_name()
        .ok_or_else(|| AppError::Config("无效的文件路径".to_string()))?
        .to_string_lossy()
        .to_string();

    let metadata = tokio::fs::metadata(&file_path).await?;
    let file_size = metadata.len();

    // 计算 MD5 (使用阻塞线程避免UI卡顿)
    let path_clone = file_path.clone();
    let md5_hash = tokio::task::spawn_blocking(move || -> Result<String, AppError> {
        let file = File::open(&path_clone)?;
        let mut reader = BufReader::new(file);
        let mut hasher = Md5::new();
        let mut buffer = [0u8; 8192];

        loop {
            let count = reader.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            hasher.update(&buffer[..count]);
        }

        Ok(hex::encode(hasher.finalize()))
    })
    .await
    .map_err(|e| AppError::Config(format!("计算任务失败: {}", e)))??;

    Ok(FileInfo {
        file_name,
        file_size,
        md5_hash,
    })
}

// Tauri 命令 - 返回 String 错误以便序列化
#[tauri::command]
async fn get_file_info(file_path: String) -> Result<FileInfo, String> {
    get_file_info_impl(file_path).await.map_err(|e| e.to_string())
}

// 内部实现
async fn upload_file_impl(
    file_path: String,
    bucket: String,
    key: String,
    region: String,
    access_key: String,
    secret_key: String,
) -> Result<UploadResult, AppError> {
    // 获取文件信息
    let file_info = get_file_info_impl(file_path.clone()).await?;

    // 计算 Base64 MD5 (S3 Content-MD5 头需要)
    let path_clone = file_path.clone();
    let md5_base64 = tokio::task::spawn_blocking(move || -> Result<String, AppError> {
        let file = File::open(&path_clone)?;
        let mut reader = BufReader::new(file);
        let mut hasher = Md5::new();
        let mut buffer = [0u8; 8192];

        loop {
            let count = reader.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            hasher.update(&buffer[..count]);
        }

        Ok(general_purpose::STANDARD.encode(hasher.finalize()))
    })
    .await
    .map_err(|e| AppError::Config(format!("计算任务失败: {}", e)))??;

    // 配置 AWS 客户端
    let creds = aws_sdk_s3::config::Credentials::new(
        access_key,
        secret_key,
        None,
        None,
        "manual-input",
    );

    let config = aws_sdk_s3::config::Builder::new()
        .region(Region::new(region))
        .credentials_provider(creds)
        .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
        .build();

    let client = Client::from_conf(config);

    // 读取文件流
    let body = ByteStream::from_path(Path::new(&file_path))
        .await
        .map_err(|e| AppError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;

    // 执行上传 - 使用 match 处理错误
    let resp = match client
        .put_object()
        .bucket(&bucket)
        .key(&key)
        .body(body)
        .content_md5(&md5_base64)
        .content_length(file_info.file_size as i64)
        .send()
        .await {
            Ok(response) => response,
            Err(e) => {
                return Err(AppError::S3(format!("上传失败: {}", e)));
            }
        };

    Ok(UploadResult {
        file_name: file_info.file_name,
        file_size: file_info.file_size,
        md5_hash: file_info.md5_hash,
        s3_key: key,
        etag: resp.e_tag.unwrap_or_default(),
    })
}

// Tauri 命令 - 返回 String 错误以便序列化
#[tauri::command]
async fn upload_file_to_s3(
    file_path: String,
    bucket: String,
    key: String,
    region: String,
    access_key: String,
    secret_key: String,
) -> Result<UploadResult, String> {
    upload_file_impl(file_path, bucket, key, region, access_key, secret_key)
        .await
        .map_err(|e| e.to_string())
}

fn main() {
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_opener::init())
        .invoke_handler(tauri::generate_handler![
            get_file_info,
            upload_file_to_s3
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
