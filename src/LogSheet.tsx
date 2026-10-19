import { useEffect, useRef } from "react";
import { Sheet } from "./Sheet";
import { MAX_SESSION_LOGS, type LogLevel, type SessionLog } from "./useSessionLogs";

const levelLabels: Record<LogLevel, string> = {
  info: "信息",
  success: "成功",
  warning: "提醒",
  error: "错误",
};

interface LogSheetProps {
  open: boolean;
  logs: SessionLog[];
  onClose: () => void;
  onClear: () => void;
}

export function LogSheet({ open, logs, onClose, onClear }: LogSheetProps) {
  const listRef = useRef<HTMLDivElement>(null);
  const followLatest = useRef(true);

  useEffect(() => {
    if (open) followLatest.current = true;
  }, [open]);

  useEffect(() => {
    const list = listRef.current;
    if (open && list && followLatest.current) list.scrollTop = list.scrollHeight;
  }, [open, logs]);

  return (
    <Sheet id="session-log-sheet" title="运行日志" description={`仅保留本次运行的最近 ${MAX_SESSION_LOGS} 条日志，退出软件后自动清空。`} open={open} onClose={onClose}>
      <div className="sheet-toolbar">
        <span>{logs.length} 条日志</span>
        <button type="button" className="btn btn-secondary" disabled={logs.length === 0} onClick={onClear}>清空日志</button>
      </div>
      <div ref={listRef} className="sheet-content log-list" role="log" aria-label="本次运行日志" aria-live={open ? "polite" : "off"} onScroll={(event) => {
        const list = event.currentTarget;
        followLatest.current = list.scrollHeight - list.scrollTop - list.clientHeight < 40;
      }}>
        {logs.length === 0 ? <p className="log-empty">暂无日志，后续操作会记录在这里。</p> : logs.map((entry) => (
          <article key={entry.id} className={`log-entry log-entry-${entry.level}`}>
            <div className="log-entry-meta">
              <time dateTime={entry.timestamp}>{new Date(entry.timestamp).toLocaleTimeString("zh-CN", { hour12: false })}</time>
              <span className="log-level">{levelLabels[entry.level]}</span>
            </div>
            <p>{entry.message}</p>
          </article>
        ))}
      </div>
    </Sheet>
  );
}
