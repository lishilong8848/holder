from __future__ import annotations

import json
import logging
import threading
import uuid
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional


TASK_STATUS_PENDING = "pending"
TASK_STATUS_RUNNING = "running"
TASK_STATUS_SUCCESS = "success"
TASK_STATUS_FAILED = "failed"
TASK_STATUS_ENDED = "ended"
ACTIVE_TASK_STATUSES = {TASK_STATUS_PENDING, TASK_STATUS_RUNNING}
TERMINAL_TASK_STATUSES = {TASK_STATUS_SUCCESS, TASK_STATUS_FAILED, TASK_STATUS_ENDED}

EVENT_LEVEL_ORDER = {
    "debug": 10,
    "info": 20,
    "warning": 30,
    "error": 40,
    "critical": 50,
}


def now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


@dataclass
class TaskRecord:
    task_id: str
    workflow: str
    record_id: str
    source: str
    status: str = TASK_STATUS_PENDING
    current_step: str = "已创建"
    progress_current: int = 0
    progress_total: int = 0
    started_at: str = field(default_factory=now_iso)
    finished_at: Optional[str] = None
    error: str = ""
    summary: Dict[str, Any] = field(default_factory=dict)
    details: List[Dict[str, Any]] = field(default_factory=list)
    updated_at: str = field(default_factory=now_iso)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class TaskRegistry:
    def __init__(self, history_path: Path, *, max_tasks: int = 300):
        self.history_path = Path(history_path)
        self.max_tasks = max_tasks
        self._lock = threading.RLock()
        self._tasks: Dict[str, TaskRecord] = {}
        self._order: Deque[str] = deque()
        self._events: Deque[Dict[str, Any]] = deque(maxlen=1000)
        self._load_history()

    def _load_history(self) -> None:
        if not self.history_path.exists():
            return

        latest: Dict[str, TaskRecord] = {}
        try:
            for line in self.history_path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                payload = json.loads(line)
                task_id = str(payload.get("task_id") or "").strip()
                if not task_id:
                    continue
                latest[task_id] = TaskRecord(**payload)
        except Exception as exc:
            self.add_event("system", f"任务历史读取失败: {exc}", level="error")
            return

        for task in sorted(latest.values(), key=lambda item: item.updated_at):
            if task.status in ACTIVE_TASK_STATUSES:
                task.status = TASK_STATUS_ENDED
                task.current_step = "服务已重启，历史任务按已结束处理"
                task.finished_at = task.finished_at or task.updated_at or now_iso()
                task.updated_at = task.finished_at
            self._tasks[task.task_id] = task
            self._order.append(task.task_id)
        self._trim_locked()

    def _persist_locked(self, task: TaskRecord) -> None:
        self.history_path.parent.mkdir(parents=True, exist_ok=True)
        with self.history_path.open("a", encoding="utf-8") as fp:
            fp.write(json.dumps(task.to_dict(), ensure_ascii=False) + "\n")

    def _trim_locked(self) -> None:
        while len(self._order) > self.max_tasks:
            old_id = self._order.popleft()
            self._tasks.pop(old_id, None)

    def add_event(self, source: str, message: str, *, level: str = "info") -> None:
        event = {
            "time": now_iso(),
            "source": source,
            "level": level,
            "message": str(message),
        }
        with self._lock:
            self._events.append(event)

    def create_task(
        self,
        *,
        workflow: str,
        record_id: str,
        source: str,
        task_id: Optional[str] = None,
        current_step: str = "已创建",
        progress_total: int = 0,
        summary: Optional[Dict[str, Any]] = None,
    ) -> TaskRecord:
        task = TaskRecord(
            task_id=task_id or uuid.uuid4().hex,
            workflow=workflow,
            record_id=record_id,
            source=source,
            status=TASK_STATUS_RUNNING,
            current_step=current_step,
            progress_total=progress_total,
            summary=summary or {},
        )
        with self._lock:
            self._tasks[task.task_id] = task
            self._order.append(task.task_id)
            self._trim_locked()
            self._persist_locked(task)
            self.add_event(workflow, f"任务已创建: {record_id}")
        return task

    def update_task(
        self,
        task_id: Optional[str],
        *,
        status: Optional[str] = None,
        current_step: Optional[str] = None,
        progress_current: Optional[int] = None,
        progress_total: Optional[int] = None,
        error: Optional[str] = None,
        summary: Optional[Dict[str, Any]] = None,
    ) -> Optional[TaskRecord]:
        if not task_id:
            return None
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                return None
            if status is not None:
                task.status = status
            if current_step is not None:
                task.current_step = current_step
            if progress_current is not None:
                task.progress_current = max(0, int(progress_current))
            if progress_total is not None:
                task.progress_total = max(0, int(progress_total))
            if error is not None:
                task.error = error
            if summary:
                task.summary.update(summary)
            task.updated_at = now_iso()
            self._persist_locked(task)
            return task

    def add_detail(
        self,
        task_id: Optional[str],
        *,
        label: str,
        status: str,
        message: str = "",
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        if not task_id:
            return
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                return
            task.details.append(
                {
                    "time": now_iso(),
                    "label": label,
                    "status": status,
                    "message": message,
                    **(extra or {}),
                }
            )
            task.updated_at = now_iso()
            self._persist_locked(task)

    def finish_task(
        self,
        task_id: Optional[str],
        *,
        status: str,
        current_step: str,
        error: str = "",
        summary: Optional[Dict[str, Any]] = None,
    ) -> Optional[TaskRecord]:
        if not task_id:
            return None
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                return None
            task.status = status
            task.current_step = current_step
            task.error = error
            task.finished_at = now_iso()
            task.updated_at = task.finished_at
            if summary:
                task.summary.update(summary)
            self._persist_locked(task)
            self.add_event(task.workflow, f"任务结束: {task.record_id} / {status}")
            return task

    def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            task = self._tasks.get(task_id)
            return task.to_dict() if task else None

    def list_tasks(self, *, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            task_ids = list(self._order)[-max(1, limit):]
            return [self._tasks[task_id].to_dict() for task_id in reversed(task_ids) if task_id in self._tasks]

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            tasks = list(self._tasks.values())
        running = sum(1 for task in tasks if task.status in ACTIVE_TASK_STATUSES)
        success = sum(1 for task in tasks if task.status == TASK_STATUS_SUCCESS)
        failed = sum(1 for task in tasks if task.status == TASK_STATUS_FAILED)
        ended = sum(1 for task in tasks if task.status == TASK_STATUS_ENDED)
        by_workflow: Dict[str, int] = {}
        for task in tasks:
            by_workflow[task.workflow] = by_workflow.get(task.workflow, 0) + 1
        return {
            "total": len(tasks),
            "running": running,
            "success": success,
            "failed": failed,
            "ended": ended,
            "by_workflow": by_workflow,
        }

    def events(self, *, limit: int = 300, min_level: str = "warning") -> List[Dict[str, Any]]:
        min_level_value = EVENT_LEVEL_ORDER.get(str(min_level or "warning").lower(), EVENT_LEVEL_ORDER["warning"])
        with self._lock:
            filtered = [
                event
                for event in self._events
                if EVENT_LEVEL_ORDER.get(str(event.get("level") or "info").lower(), EVENT_LEVEL_ORDER["info"]) >= min_level_value
            ]
            return filtered[-max(1, limit):]


class MemoryLogHandler(logging.Handler):
    def __init__(self, registry: TaskRegistry):
        super().__init__()
        self.registry = registry

    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = self.format(record)
        except Exception:
            message = record.getMessage()
        self.registry.add_event(record.name, message, level=record.levelname.lower())


project_root = Path(__file__).resolve().parents[1]
TASK_REGISTRY = TaskRegistry(project_root / "output" / "ui_tasks" / "tasks.jsonl")


def install_memory_log_handler() -> None:
    root = logging.getLogger()
    for handler in root.handlers:
        if isinstance(handler, MemoryLogHandler):
            return
    handler = MemoryLogHandler(TASK_REGISTRY)
    handler.setFormatter(logging.Formatter("%(levelname)s:%(name)s:%(message)s"))
    root.addHandler(handler)
