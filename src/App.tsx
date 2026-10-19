import { useCallback, useEffect, useState, useRef } from "react";
import { Channel, invoke } from "@tauri-apps/api/core";
import { open } from "@tauri-apps/plugin-dialog";
import { openUrl } from "@tauri-apps/plugin-opener";
import { LogSheet } from "./LogSheet";
import { Sheet } from "./Sheet";
import { useSessionLogs } from "./useSessionLogs";
import "./App.css";

interface FileInfo {
  file_name: string;
  file_size: number;
  modified_at: string;
}

interface UploadResult {
  file_info: FileInfo;
  s3_key: string;
  file_url: string;
  etag: string;
  verification: "sha256" | "sha256_composite" | "download_sha256" | "size_only";
}

interface Md5Result {
  md5_hash: string;
  md5_url: string;
}

interface UploadTask {
  id: string;
  file_path: string;
  file_info: FileInfo;
  key: string;
  checksum: boolean;
  phase: string;
  last_error: string | null;
  upload_id: string | null;
  target: { bucket: string; region: string; endpoint: string | null; force_path_style: boolean };
}

interface UploadOutcome {
  status: "complete" | "paused" | "cancelled";
  result: UploadResult | null;
}

interface UploadProgress {
  task_id: string;
  phase: string;
  uploaded_bytes: number;
  total_bytes: number;
}

interface S3Config {
  bucket: string;
  region: string;
  endpoint: string | null;
  key_prefix: string;
}

type BusyAction = "selecting" | "uploading" | "hashing" | "cleaning" | null;

function formatBytes(bytes: number) {
  if (bytes === 0) return "0 B";
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  const index = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
  return `${Number((bytes / 1024 ** index).toFixed(2))} ${units[index]}`;
}

const phaseLabels: Record<string, string> = {
  preparing: "正在准备上传…",
  checking_local: "正在核对本地文件与已上传分片…",
  uploading: "正在传输（失败分片将自动重试）",
  completing: "正在合并分片，请等待服务器确认…",
  verifying: "正在校验远端对象…",
  verifying_download: "上传已完成，正在下载并校验文件内容…",
  complete: "上传完成",
  paused: "已暂停，可以继续上传",
  cancelling: "正在取消并清理远端分片…",
  cancelled: "已取消",
  failed: "上传未完成，可从「未完成的上传」中继续",
};

function App() {
  const { logs, addLog, clearLogs } = useSessionLogs();
  const [activeSheet, setActiveSheet] = useState<"logs" | "tasks" | null>(null);
  const [filePath, setFilePath] = useState("");
  const [fileInfo, setFileInfo] = useState<FileInfo | null>(null);
  const [busyAction, setBusyAction] = useState<BusyAction>(null);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");
  const [s3Config, setS3Config] = useState<S3Config | null>(null);
  const [configError, setConfigError] = useState("");
  const [isConfigLoading, setIsConfigLoading] = useState(true);
  const [customKey, setCustomKey] = useState("");
  const [progress, setProgress] = useState<UploadProgress | null>(null);
  const [uploadResult, setUploadResult] = useState<UploadResult | null>(null);
  const [md5Result, setMd5Result] = useState<Md5Result | null>(null);
  const [tasks, setTasks] = useState<UploadTask[]>([]);
  const [tasksLoading, setTasksLoading] = useState(true);
  const [tasksError, setTasksError] = useState("");
  const [taskActionError, setTaskActionError] = useState("");
  const [taskNotice, setTaskNotice] = useState("");
  const [checksum, setChecksum] = useState(true);
  const [controlPending, setControlPending] = useState(false);
  const activeTask = useRef<string | null>(null);
  const initialized = useRef(false);
  const isBusy = busyAction !== null || isConfigLoading || tasksLoading;

  const loadTasks = useCallback(async () => {
    setTasksLoading(true);
    try {
      const listing = await invoke<{ tasks: UploadTask[]; warnings: string[] }>("list_upload_tasks");
      setTasks(listing.tasks);
      setTasksError(listing.warnings.join("；"));
      addLog(`任务列表已更新：${listing.tasks.length} 个未完成任务`);
      listing.warnings.forEach((warning) => addLog(warning, "warning"));
    } catch (err) {
      setTasksError(`读取恢复任务失败：${err}`);
      addLog(`读取恢复任务失败：${err}`, "error");
    } finally {
      setTasksLoading(false);
    }
  }, [addLog]);

  const loadS3Config = useCallback(async () => {
    setIsConfigLoading(true);
    try {
      setS3Config(await invoke<S3Config>("get_s3_config"));
      setConfigError("");
      addLog("存储配置加载成功", "success");
    } catch (err) {
      setS3Config(null);
      setConfigError(String(err));
      addLog(`存储配置加载失败：${err}`, "error");
    } finally {
      setIsConfigLoading(false);
    }
  }, [addLog]);

  useEffect(() => {
    if (initialized.current) return;
    initialized.current = true;
    addLog("应用已启动");
    void loadS3Config();
    void loadTasks();
  }, [addLog, loadS3Config, loadTasks]);

  const handleSelectFile = async () => {
    setBusyAction("selecting");
    setError("");
    setNotice("");
    try {
      const selected = await open({ multiple: false, directory: false });
      if (typeof selected !== "string") return;
      setFilePath(selected);
      setFileInfo(null);
      setUploadResult(null);
      setMd5Result(null);
      setProgress(null);
      setCustomKey("");
      const info = await invoke<FileInfo>("get_file_info", { filePath: selected });
      setFileInfo(info);
      addLog(`已选择文件：${info.file_name}（${formatBytes(info.file_size)}）`);
      const prefix = s3Config?.key_prefix;
      setCustomKey(prefix ? `${prefix}/${info.file_name}` : info.file_name);
    } catch (err) {
      setError(`选择文件失败：${err}`);
      addLog(`选择文件失败：${err}`, "error");
    } finally {
      setBusyAction(null);
    }
  };

  const handleUpload = async (task?: UploadTask) => {
    if (activeTask.current) return;
    if (!task && (!fileInfo || !s3Config || !customKey.trim())) return;
    const taskId = task?.id ?? crypto.randomUUID();
    const info = task?.file_info ?? fileInfo!;
    addLog(`${task ? "继续上传" : "开始上传"}：${info.file_name}（${formatBytes(info.file_size)}）`);
    activeTask.current = taskId;
    setBusyAction("uploading");
    setControlPending(false);
    setError("");
    setNotice("");
    setUploadResult(null);
    setMd5Result(null);
    if (task) {
      setActiveSheet(null);
      setFilePath(task.file_path);
      setFileInfo(task.file_info);
      setCustomKey(task.key);
      setChecksum(task.checksum);
    }
    setProgress({ task_id: taskId, phase: "preparing", uploaded_bytes: 0, total_bytes: info.file_size });
    let lastPhase = "";
    const onProgress = new Channel<UploadProgress>((event) => {
      if (activeTask.current === taskId && event.task_id === taskId) {
        setProgress(event);
        // Byte updates are frequent; record stage changes without flooding the log.
        if (event.phase !== lastPhase) {
          lastPhase = event.phase;
          addLog(`${info.file_name}：${phaseLabels[event.phase] ?? event.phase}`);
        }
      }
    });
    try {
      const outcome = await invoke<UploadOutcome>(task ? "resume_upload" : "upload_file_to_s3", task
        ? { taskId, onProgress }
        : { taskId, filePath, key: customKey, checksum, onProgress });
      if (outcome.result) {
        const result = outcome.result;
        setFileInfo(result.file_info);
        setProgress({ task_id: taskId, phase: "complete", uploaded_bytes: result.file_info.file_size, total_bytes: result.file_info.file_size });
        setUploadResult(result);
        addLog(`上传完成：${result.file_info.file_name}（${result.verification === "size_only" ? "仅核对远端大小" : "SHA-256 校验通过"}）`, result.verification === "size_only" ? "warning" : "success");
      } else {
        setProgress((previous) => previous && ({ ...previous, phase: outcome.status }));
        setNotice(outcome.status === "paused" ? "任务已保存，可以从「未完成的上传」中继续。" : "任务已取消；已提交的远端对象会保留。");
        addLog(outcome.status === "paused" ? "上传已暂停，任务已保存" : "上传已取消；已提交的远端对象会保留", "warning");
      }
    } catch (err) {
      setProgress((previous) => previous && ({ ...previous, phase: "failed" }));
      setError(`上传未完成：${err}`);
      addLog(`上传未完成：${err}`, "error");
    } finally {
      activeTask.current = null;
      setControlPending(false);
      await loadTasks();
      setBusyAction(null);
    }
  };

  const handleControl = async (action: "pause" | "cancel") => {
    const taskId = activeTask.current;
    if (!taskId) return;
    setControlPending(true);
    addLog(action === "pause" ? "请求暂停上传" : "请求取消上传并清理分片");
    try {
      await invoke("control_upload", { taskId, action });
      if (activeTask.current === taskId) setNotice(action === "pause" ? "正在暂停，请等待任务保存…" : "正在取消，请等待远端分片清理…");
    } catch (err) {
      if (activeTask.current === taskId) {
        setError(String(err));
        addLog(`${action === "pause" ? "暂停" : "取消"}请求失败：${err}`, "error");
        setControlPending(false);
      }
    }
  };

  const handleDiscard = async (selectedTasks: UploadTask[]) => {
    if (isBusy || selectedTasks.length === 0) return;
    setBusyAction("cleaning");
    setTaskActionError("");
    setTaskNotice("");
    const cleaned = new Set<string>();
    const failures: string[] = [];
    try {
      // The backend holds a store writer lock, so cleanup requests must run in order.
      for (const [index, task] of selectedTasks.entries()) {
        setTaskNotice(`正在清理：${task.file_info.file_name}（${index + 1}/${selectedTasks.length}）`);
        addLog(`开始清理任务：${task.file_info.file_name}`);
        try {
          await invoke("discard_upload", { taskId: task.id });
          cleaned.add(task.id);
          addLog(`任务记录和未完成分片已清理：${task.file_info.file_name}`, "success");
        } catch (err) {
          const message = `${task.file_info.file_name}：${err}`;
          failures.push(message);
          addLog(`清理失败：${message}`, "error");
        }
      }
      setTasks((previous) => previous.filter((task) => !cleaned.has(task.id)));
      setTaskNotice(`已清理 ${cleaned.size} 个任务；已提交的远端对象会保留。`);
      if (failures.length > 0) setTaskActionError(`${failures.length} 个任务的清理请求失败，请检查刷新后的列表并重试：${failures.join("；")}`);
    } finally {
      await loadTasks();
      setBusyAction(null);
    }
  };

  const handleCalculateMd5 = async () => {
    if (!uploadResult) return;
    setBusyAction("hashing");
    setError("");
    setNotice("");
    setMd5Result(null);
    try {
      addLog(`开始计算 MD5：${uploadResult.file_info.file_name}`);
      setMd5Result(await invoke<Md5Result>("calculate_md5", {
        filePath,
        expectedSize: uploadResult.file_info.file_size,
        expectedModified: uploadResult.file_info.modified_at,
        fileUrl: uploadResult.file_url,
      }));
      addLog("MD5 计算完成", "success");
    } catch (err) {
      setError(`MD5 计算失败：${err}`);
      addLog(`MD5 计算失败：${err}`, "error");
    } finally {
      setBusyAction(null);
    }
  };

  const copyText = async (value: string) => {
    try {
      await navigator.clipboard.writeText(value);
      setNotice("已复制链接");
      addLog("已复制链接", "success");
    } catch {
      setError("复制失败，请选中链接手动复制");
      addLog("复制失败，请选中链接手动复制", "error");
    }
  };

  const handleOpenUrl = async (url: string) => {
    try {
      await openUrl(url);
      addLog("已打开文件链接");
    } catch (err) {
      setError(`打开链接失败：${err}`);
      addLog(`打开链接失败：${err}`, "error");
    }
  };

  const renderLink = (url: string, label: string) => (
    <div className="result-link">
      <a href={url} onClick={(event) => { event.preventDefault(); void handleOpenUrl(url); }}>{url}</a>
      <button className="btn btn-secondary" onClick={() => void copyText(url)} aria-label={`复制${label}`}>复制链接</button>
    </div>
  );

  const percent = uploadResult ? 100 : progress && progress.total_bytes > 0
    ? Math.min(100, progress.uploaded_bytes / progress.total_bytes * 100) : 0;
  const canControl = activeTask.current !== null && busyAction === "uploading" && !controlPending
    && !["completing", "verifying", "cancelling", "complete"].includes(progress?.phase ?? "");

  return (
    <main className="container">
      <header>
        <div className="page-heading">
          <h1>OTACast 文件上传</h1>
          <div className="page-actions">
            <button type="button" className="btn btn-secondary" aria-haspopup="dialog" aria-controls="upload-tasks-sheet" aria-expanded={activeSheet === "tasks"} onClick={() => setActiveSheet("tasks")}>未完成的上传（{tasksLoading ? "…" : tasks.length}）{tasksError && " · 需检查"}</button>
            <button type="button" className="btn btn-secondary" aria-haspopup="dialog" aria-controls="session-log-sheet" aria-expanded={activeSheet === "logs"} onClick={() => setActiveSheet("logs")}>运行日志（{logs.length}）</button>
          </div>
        </div>
        <p>选择文件，上传后获取链接，再按需计算 MD5。</p>
      </header>

      <section className="card">
        <h2><span className="step">1</span>选择文件</h2>
        <button onClick={handleSelectFile} disabled={isBusy} className="btn btn-primary">
          {busyAction === "selecting" ? "正在读取文件信息…" : fileInfo ? "重新选择文件" : "选择文件"}
        </button>
        {fileInfo && (
          <div className="file-details">
            <strong className="file-name">{fileInfo.file_name}</strong>
            <span>{formatBytes(fileInfo.file_size)}</span>
            <p className="local-path">{filePath}</p>
          </div>
        )}
        <details className="storage-settings">
          <summary>上传路径与存储配置</summary>
          {isConfigLoading ? <p role="status">正在加载配置…</p> : s3Config && (
            <p className="storage-summary">{s3Config.bucket} · {s3Config.region}{s3Config.endpoint && ` · ${s3Config.endpoint}`}</p>
          )}
          <button onClick={loadS3Config} disabled={isBusy} className="btn btn-secondary">重新加载配置</button>
          <div className="form-group">
            <label htmlFor="object-key">上传路径（Key）</label>
            <input id="object-key" value={customKey} onChange={(event) => setCustomKey(event.target.value)} disabled={isBusy || !fileInfo} placeholder="选择文件后自动生成" />
          </div>
          <label className="checksum-setting">
            <input type="checkbox" checked={checksum} onChange={(event) => setChecksum(event.target.checked)} disabled={isBusy} />
            上传后验证文件内容（SHA-256，默认开启）
          </label>
          {checksum && <p className="helper-text">优先核对远端摘要；存储服务不返回摘要时，会下载文件验证内容，产生相应下载流量。</p>}
          {!checksum && <p className="verification-warning">关闭后只核对文件大小，不验证内容完整性。</p>}
          <p className="helper-text">大于 128MiB 的文件自动分片上传，支持暂停和退出后继续。</p>
        </details>
        {configError && <p role="alert" className="inline-error">配置错误：{configError}。请更新 .env 后重新加载配置。</p>}
      </section>

      <section className={`card ${uploadResult ? "card-complete" : ""}`}>
        <h2><span className="step">2</span>{uploadResult ? "上传完成" : "上传文件"}</h2>
        <button onClick={() => void handleUpload()} disabled={isBusy || !fileInfo || !s3Config || !customKey.trim()} className="btn btn-success btn-large">
          {busyAction === "uploading" ? "正在上传…" : uploadResult ? "重新上传" : "开始上传"}
        </button>
        {busyAction === "uploading" && (
          <div className="upload-actions">
            <button className="btn btn-secondary" onClick={() => void handleControl("pause")} disabled={!canControl}>暂停并保存</button>
            <button className="btn btn-danger" onClick={() => void handleControl("cancel")} disabled={!canControl}>取消并清理</button>
          </div>
        )}
        {progress && (
          <div className="upload-progress" aria-live="polite">
            <div className="progress-summary">
              <span>{phaseLabels[progress.phase] ?? progress.phase}</span>
              <strong>{percent.toFixed(1)}%</strong>
            </div>
            <progress max={100} value={percent} aria-label="上传进度" />
            <p className="progress-bytes">{formatBytes(progress.uploaded_bytes)} / {formatBytes(progress.total_bytes)}</p>
          </div>
        )}
        {uploadResult && (
          <div className="upload-result">
            <p className={uploadResult.verification === "size_only" ? "verification-warning" : "verification-success"}>
              {uploadResult.verification === "sha256_composite" ? "远端 SHA-256 分片组合校验通过" : uploadResult.verification === "sha256" ? "远端 SHA-256 校验通过" : uploadResult.verification === "download_sha256" ? "下载内容 SHA-256 校验通过（存储服务未返回远端摘要）" : "仅核对远端大小，未验证内容完整性"}
            </p>
            <h3>文件 URL</h3>
            {renderLink(uploadResult.file_url, "文件 URL")}
          </div>
        )}
      </section>

      {uploadResult && (
        <section className="card">
          <h2><span className="step">3</span>计算 MD5</h2>
          <p className="helper-text">计算文件 MD5，并生成带 MD5 参数的文件链接。</p>
          <button onClick={handleCalculateMd5} disabled={isBusy} className="btn btn-primary">
            {busyAction === "hashing" ? "正在计算 MD5…" : md5Result ? "重新计算 MD5" : "计算 MD5"}
          </button>
          {busyAction === "hashing" && <p role="status" className="helper-text">正在读取文件，请稍候…</p>}
          {md5Result && (
            <div className="md5-result">
              <h3>MD5</h3>
              <code className="hash">{md5Result.md5_hash}</code>
              <h3>MD5 URL</h3>
              {renderLink(md5Result.md5_url, "MD5 URL")}
            </div>
          )}
        </section>
      )}
      {error && <div role="alert" className="status-box error">{error}</div>}
      {notice && <p role="status" className="copy-notice">{notice}</p>}
      <Sheet id="upload-tasks-sheet" title="未完成的上传" description="继续上传，或清理任务记录和未完成分片。清理后无法续传，已提交的远端对象会保留。" open={activeSheet === "tasks"} onClose={() => setActiveSheet(null)}>
        <div className="sheet-toolbar task-toolbar">
          <span>{tasks.length} 个任务</span>
          <div className="page-actions">
            <button type="button" className="btn btn-secondary" disabled={isBusy} onClick={() => void loadTasks()}>刷新任务</button>
            <button type="button" className="btn btn-danger" disabled={isBusy || tasks.length === 0 || (!s3Config && tasks.every((task) => task.upload_id !== null))} onClick={() => void handleDiscard(tasks)}>{busyAction === "cleaning" ? "正在清理…" : "清理全部任务"}</button>
          </div>
        </div>
        <div className="sheet-content task-list" aria-busy={tasksLoading || busyAction === "cleaning"}>
          {tasksLoading && <p role="status" className="helper-text">正在检查任务和待清理分片…</p>}
          {tasksError && <p role="alert" className="inline-error">{tasksError}</p>}
          {taskActionError && <p role="alert" className="inline-error">{taskActionError}</p>}
          {taskNotice && <p role="status" className="copy-notice">{taskNotice}</p>}
          {!s3Config && <p className="helper-text">没有远端分片的任务可直接清理；继续上传或清理远端分片需要加载任务原来的存储配置。</p>}
          {!tasksLoading && !tasksError && tasks.length === 0 && <p className="log-empty">暂无未完成的上传。</p>}
          {tasks.map((task) => (
            <article className="recovery-task" key={task.id}>
              <strong>{task.file_info.file_name}</strong>
              <p>{formatBytes(task.file_info.file_size)} · {task.target.bucket} · {task.key}</p>
              <p>区域：{task.target.region} · 寻址方式：{task.target.force_path_style ? "路径（S3_FORCE_PATH_STYLE=true）" : "虚拟主机（S3_FORCE_PATH_STYLE=false）"}</p>
              <p>服务地址：{task.target.endpoint ?? "AWS 默认地址（S3_ENDPOINT 留空）"}</p>
              <p className="local-path">{task.file_path}</p>
              <p className="helper-text">{task.upload_id === null ? "无已记录的远端分片，可直接清理任务记录。" : "有远端分片，清理时需使用以上存储配置及有效凭证。"}</p>
              <p>{task.phase === "cleanup_pending" ? "等待清理" : ["completing", "verifying"].includes(task.phase) ? "提交结果待确认，继续后先查询远端" : "可继续上传"}</p>
              {task.last_error && <p className="inline-error">{task.last_error}</p>}
              <div className="upload-actions">
                <button type="button" className="btn btn-primary" disabled={isBusy || !s3Config || task.phase === "cleanup_pending"} onClick={() => void handleUpload(task)}>继续上传 / 校验</button>
                <button type="button" className="btn btn-danger" disabled={isBusy || (!s3Config && task.upload_id !== null)} onClick={() => void handleDiscard([task])}>放弃并清理</button>
              </div>
            </article>
          ))}
        </div>
      </Sheet>
      <LogSheet open={activeSheet === "logs"} logs={logs} onClose={() => setActiveSheet(null)} onClear={clearLogs} />
    </main>
  );
}

export default App;
