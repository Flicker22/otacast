import { useState } from "react";
import { invoke } from "@tauri-apps/api/core";
import { open } from "@tauri-apps/plugin-dialog";
import "./App.css";

interface FileInfo {
  file_name: string;
  file_size: number;
  md5_hash: string;
}

interface UploadResult {
  file_name: string;
  file_size: number;
  md5_hash: string;
  s3_key: string;
  etag: string;
}

function App() {
  const [filePath, setFilePath] = useState("");
  const [fileInfo, setFileInfo] = useState<FileInfo | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [status, setStatus] = useState("");

  // S3 配置
  const [bucket, setBucket] = useState("");
  const [region, setRegion] = useState("us-east-1");
  const [accessKey, setAccessKey] = useState("");
  const [secretKey, setSecretKey] = useState("");
  const [folder, setFolder] = useState("");
  const [customKey, setCustomKey] = useState("");

  const handleSelectFile = async () => {
    try {
      const selected = await open({
        multiple: false,
        directory: false,
      });

      if (selected && typeof selected === "string") {
        setFilePath(selected);
        setStatus("正在计算文件 MD5...");
        setFileInfo(null);
        
        const info: FileInfo = await invoke("get_file_info", {
          filePath: selected,
        });
        
        setFileInfo(info);
        setStatus("文件信息计算完成");
        
        // 自动生成 S3 Key
        if (!customKey) {
          const prefix = folder ? folder.replace(/\/$/, "") + "/" : "";
          setCustomKey(prefix + info.file_name);
        }
      }
    } catch (err) {
      setStatus(`错误: ${err}`);
    }
  };

  const handleUpload = async () => {
    if (!fileInfo || !bucket || !accessKey || !secretKey) {
      setStatus("请填写完整配置信息");
      return;
    }

    setIsLoading(true);
    setStatus("正在上传至 S3...");

    try {
      const result: UploadResult = await invoke("upload_file_to_s3", {
        filePath,
        bucket,
        key: customKey,
        region,
        accessKey,
        secretKey,
      });

      setStatus(
        `✅ 上传成功!\n` +
        `📁 文件: ${result.file_name}\n` +
        `📦 大小: ${formatBytes(result.file_size)}\n` +
        `🔑 MD5: ${result.md5_hash}\n` +
        `📍 S3路径: ${result.s3_key}\n` +
        `🏷️ ETag: ${result.etag}`
      );
    } catch (err) {
      setStatus(`❌ 上传失败: ${err}`);
    } finally {
      setIsLoading(false);
    }
  };

  const formatBytes = (bytes: number) => {
    if (bytes === 0) return "0 Bytes";
    const k = 1024;
    const sizes = ["Bytes", "KB", "MB", "GB", "TB"];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + " " + sizes[i];
  };

  return (
    <div className="container">
      <header>
        <h1>🚀 OTACast S3 上传工具</h1>
        <p>计算文件 MD5 并上传至 AWS S3</p>
      </header>

      <section className="card">
        <h2>1. 选择文件</h2>
        <button 
          onClick={handleSelectFile} 
          disabled={isLoading}
          className="btn btn-primary"
        >
          📂 选择文件
        </button>
        
        {filePath && (
          <div className="info-box">
            <p><strong>本地路径:</strong> {filePath}</p>
          </div>
        )}

        {fileInfo && (
          <div className="file-details">
            <div className="detail-item">
              <span>文件名:</span>
              <code>{fileInfo.file_name}</code>
            </div>
            <div className="detail-item">
              <span>文件大小:</span>
              <strong>{formatBytes(fileInfo.file_size)}</strong>
              <small> ({fileInfo.file_size.toLocaleString()} bytes)</small>
            </div>
            <div className="detail-item">
              <span>MD5 哈希:</span>
              <code className="hash">{fileInfo.md5_hash}</code>
            </div>
          </div>
        )}
      </section>

      <section className="card">
        <h2>2. S3 配置</h2>
        <div className="form-grid">
          <div className="form-group">
            <label>Bucket 名称 *</label>
            <input
              type="text"
              value={bucket}
              onChange={(e) => setBucket(e.target.value)}
              placeholder="my-bucket-name"
            />
          </div>
          
          <div className="form-group">
            <label>Region *</label>
            <input
              type="text"
              value={region}
              onChange={(e) => setRegion(e.target.value)}
              placeholder="us-east-1"
            />
          </div>

          <div className="form-group">
            <label>Access Key ID *</label>
            <input
              type="password"
              value={accessKey}
              onChange={(e) => setAccessKey(e.target.value)}
              placeholder="AKIA..."
            />
          </div>

          <div className="form-group">
            <label>Secret Access Key *</label>
            <input
              type="password"
              value={secretKey}
              onChange={(e) => setSecretKey(e.target.value)}
            />
          </div>
        </div>

        <div className="form-group" style={{ marginTop: "15px" }}>
          <label>目标文件夹 (可选)</label>
          <input
            type="text"
            value={folder}
            onChange={(e) => {
              setFolder(e.target.value);
              if (fileInfo) {
                const prefix = e.target.value.replace(/\/$/, "");
                setCustomKey(prefix ? `${prefix}/${fileInfo.file_name}` : fileInfo.file_name);
              }
            }}
            placeholder="uploads/2024/backups"
          />
        </div>

        <div className="form-group">
          <label>S3 完整路径 (Key) *</label>
          <input
            type="text"
            value={customKey}
            onChange={(e) => setCustomKey(e.target.value)}
            placeholder="path/to/file.zip"
          />
        </div>
      </section>

      <section className="card">
        <h2>3. 执行上传</h2>
        <button
          onClick={handleUpload}
          disabled={isLoading || !fileInfo}
          className="btn btn-success btn-large"
        >
          {isLoading ? "⏳ 上传中..." : "🚀 开始上传到 S3"}
        </button>
      </section>

      {status && (
        <div className={`status-box ${status.includes("错误") || status.includes("失败") ? "error" : ""}`}>
          <pre>{status}</pre>
        </div>
      )}
    </div>
  );
}

export default App;