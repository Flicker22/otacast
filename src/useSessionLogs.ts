import { useCallback, useRef, useState } from "react";

export type LogLevel = "info" | "success" | "warning" | "error";

export interface SessionLog {
  id: number;
  timestamp: string;
  level: LogLevel;
  message: string;
}

export const MAX_SESSION_LOGS = 500;

// Session logs deliberately stay in memory: quitting the app discards them.
export function useSessionLogs() {
  const [logs, setLogs] = useState<SessionLog[]>([]);
  const nextId = useRef(0);

  const addLog = useCallback((message: string, level: LogLevel = "info") => {
    const entry: SessionLog = {
      id: nextId.current++,
      timestamp: new Date().toISOString(),
      level,
      message,
    };
    setLogs((previous) => [...previous, entry].slice(-MAX_SESSION_LOGS));
  }, []);

  const clearLogs = useCallback(() => setLogs([]), []);

  return { logs, addLog, clearLogs };
}
