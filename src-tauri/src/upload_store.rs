//! Durable upload journal. Credentials are deliberately absent from this format.
use crate::{s3_config::S3Config, FileInfo};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, io::Write, path::PathBuf};

pub const PART_SIZE: u64 = 64 * 1024 * 1024;
pub const MULTIPART_THRESHOLD: u64 = 128 * 1024 * 1024;
pub const CONCURRENCY: usize = 3;

fn default_concurrency() -> usize {
    CONCURRENCY
}
fn default_remote_checksums() -> bool {
    true
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Target {
    pub bucket: String,
    pub region: String,
    pub endpoint: Option<String>,
    pub force_path_style: bool,
}

impl From<&S3Config> for Target {
    fn from(s: &S3Config) -> Self {
        Self {
            bucket: s.bucket.clone(),
            region: s.region.clone(),
            endpoint: s.endpoint.clone(),
            force_path_style: s.force_path_style,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Part {
    pub number: i32,
    pub size: u64,
    pub etag: String,
    pub sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Task {
    pub schema: u32,
    pub id: String,
    pub file_path: String,
    pub file_info: FileInfo,
    pub target: Target,
    pub key: String,
    pub file_url: String,
    pub multipart: bool,
    pub checksum: bool,
    #[serde(default = "default_remote_checksums")]
    pub remote_checksums: bool,
    pub part_size: u64,
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    pub upload_id: Option<String>,
    pub parts: BTreeMap<i32, Part>,
    pub phase: String,
    pub last_error: Option<String>,
    pub etag: Option<String>,
    pub version_id: Option<String>,
}

impl Task {
    pub fn part_count(&self) -> i32 {
        if self.multipart {
            self.file_info.file_size.div_ceil(self.part_size).max(1) as i32
        } else {
            1
        }
    }

    pub fn range(&self, number: i32) -> (u64, u64) {
        let offset = (number as u64 - 1) * self.part_size;
        (
            offset,
            self.file_info
                .file_size
                .saturating_sub(offset)
                .min(self.part_size),
        )
    }

    pub fn validate(&self) -> Result<(), String> {
        validate_id(&self.id)?;
        if self.schema != 1
            || !(5 * 1024 * 1024..=PART_SIZE).contains(&self.part_size)
            || !(1..=8).contains(&self.concurrency)
            || self.file_info.file_size > self.part_size * 10_000
            || (!self.multipart && self.file_info.file_size > MULTIPART_THRESHOLD)
        {
            return Err("上传记录版本或分片参数无效".into());
        }
        for (number, part) in &self.parts {
            if *number < 1 || *number > self.part_count() {
                return Err("上传记录的分片编号无效".into());
            }
            let expected_size = if self.multipart {
                self.range(*number).1
            } else {
                self.file_info.file_size
            };
            if part.number != *number || part.size != expected_size {
                return Err("上传记录的分片信息无效".into());
            }
            if STANDARD
                .decode(&part.sha256)
                .map_or(true, |bytes| bytes.len() != 32)
                || (self.multipart && part.etag.is_empty())
            {
                return Err("上传记录的分片摘要或 ETag 无效".into());
            }
        }
        Ok(())
    }
}

pub fn validate_id(id: &str) -> Result<(), String> {
    if id.len() != 36
        || !id.bytes().enumerate().all(|(i, c)| {
            if [8, 13, 18, 23].contains(&i) {
                c == b'-'
            } else {
                c.is_ascii_hexdigit()
            }
        })
    {
        return Err("无效的上传任务 ID".into());
    }
    Ok(())
}

#[derive(Clone)]
pub struct Store {
    pub root: PathBuf,
}

#[derive(Serialize)]
pub struct TaskList {
    pub tasks: Vec<Task>,
    pub warnings: Vec<String>,
}

impl Store {
    pub fn save(&self, task: &Task) -> Result<(), String> {
        task.validate()?;
        std::fs::create_dir_all(&self.root).map_err(|e| format!("无法创建任务目录: {e}"))?;
        let mut file = tempfile::NamedTempFile::new_in(&self.root).map_err(|e| e.to_string())?;
        serde_json::to_writer(&mut file, task).map_err(|e| e.to_string())?;
        file.flush().map_err(|e| e.to_string())?;
        file.as_file().sync_all().map_err(|e| e.to_string())?;
        file.persist(self.root.join(format!("{}.json", task.id)))
            .map_err(|e| e.to_string())?;
        // Persist the directory entry as well as the file on Unix.
        #[cfg(unix)]
        std::fs::File::open(&self.root)
            .and_then(|f| f.sync_all())
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    pub fn load(&self, id: &str) -> Result<Task, String> {
        validate_id(id)?;
        let file = std::fs::File::open(self.root.join(format!("{id}.json")))
            .map_err(|e| format!("无法读取任务: {e}"))?;
        let task: Task = serde_json::from_reader(file).map_err(|e| format!("任务记录损坏: {e}"))?;
        task.validate()?;
        if task.id != id {
            return Err("任务 ID 不匹配".into());
        }
        Ok(task)
    }

    pub fn list(&self) -> Result<TaskList, String> {
        if !self.root.exists() {
            return Ok(TaskList {
                tasks: Vec::new(),
                warnings: Vec::new(),
            });
        }
        let mut tasks = Vec::new();
        let mut warnings = Vec::new();
        for entry in std::fs::read_dir(&self.root).map_err(|e| e.to_string())? {
            let path = entry.map_err(|e| e.to_string())?.path();
            if path.extension().and_then(|s| s.to_str()) == Some("json") {
                let id = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .ok_or("任务文件名无效")?;
                match self.load(id) {
                    Ok(task) => tasks.push(task),
                    Err(error) => {
                        warnings.push(format!("任务 {id} 无法读取，已保留原文件：{error}"))
                    }
                }
            }
        }
        tasks.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(TaskList { tasks, warnings })
    }

    pub fn remove(&self, id: &str) -> Result<(), String> {
        validate_id(id)?;
        match std::fs::remove_file(self.root.join(format!("{id}.json"))) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.to_string()),
        }
    }
}
