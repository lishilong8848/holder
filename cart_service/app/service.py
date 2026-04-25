import asyncio
import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable, Deque, Dict, Generic, List, Optional, TypeVar
from concurrent.futures import ThreadPoolExecutor

from .certificate_query import (
    EFFECTIVE_END_FIELD,
    CertificateQuery,
    ExtractedCertificateCard,
    PersonQueryResult,
)
from .feishu_reader import FeishuTableReader


logger = logging.getLogger(__name__)

REVIEW_DUE_FIELD = "\u5e94\u590d\u5ba1\u65e5\u671f"
REVIEW_ACTUAL_FIELD = "\u5b9e\u9645\u590d\u5ba1\u65e5\u671f"
SKIPPED_WRITEBACK_MESSAGE = "\u67e5\u8be2\u672a\u6210\u529f\uff0c\u8df3\u8fc7\u56de\u586b"
WRITEBACK_FAILED_MESSAGE = "\u98de\u4e66\u8bb0\u5f55\u66f4\u65b0\u5931\u8d25"
LOOKUP_REQUIRED_MESSAGE = "\u672a\u4f20\u5165 people[].record_id \u65f6\uff0c\u5fc5\u987b\u63d0\u4f9b lookup \u914d\u7f6e"
LOOKUP_NOT_FOUND_MESSAGE = "\u672a\u5728\u591a\u7ef4\u8868\u4e2d\u627e\u5230\u5339\u914d\u8bb0\u5f55"
LOOKUP_MULTIPLE_MESSAGE = "\u5728\u591a\u7ef4\u8868\u4e2d\u5339\u914d\u5230\u4e86\u591a\u6761\u8bb0\u5f55"

QUERY_STATUS_LABELS = {
    "success": "\u67e5\u8be2\u6210\u529f",
    "fail_id": "\u8bc1\u4ef6\u53f7\u6709\u8bef",
    "fail_no_data": "\u672a\u67e5\u8be2\u5230\u8bc1\u4ef6\u4fe1\u606f",
    "fail_other": "\u67e5\u8be2\u5931\u8d25",
}
CERTIFICATE_TYPE_LABELS = {
    "high_voltage": "\u9ad8\u538b\u8bc1",
    "low_voltage": "\u4f4e\u538b\u8bc1",
    "refrigeration": "\u5236\u51b7\u8bc1",
    "working_at_height": "\u767b\u9ad8\u8bc1",
    "welding": "\u7126\u63a5\u8bc1",
}

T = TypeVar("T")


class QueueFullError(RuntimeError):
    """内存中的请求队列已满。"""


class QueueTimeoutError(RuntimeError):
    """请求在队列中等待超时。"""


class ClientDisconnectedError(RuntimeError):
    """客户端在执行开始前断开连接。"""


@dataclass
class QueuedRunResult(Generic[T]):
    result: T
    queued_seconds: float
    execution_seconds: float


@dataclass
class BatchPersonResult:
    name: str
    id_number: str
    success: bool
    record_id: Optional[str] = None
    query_status: Optional[str] = None
    query_error: Optional[str] = None
    writeback_error: Optional[str] = None


@dataclass
class BatchProcessResult:
    total: int
    success: int
    failed: int
    results: List[BatchPersonResult]
    query_seconds: float
    writeback_seconds: float


class BatchRequestCoordinator:
    """单进程内的单执行槽 FIFO 批量请求协调器。"""

    def __init__(self, *, max_queue_size: int, queue_timeout_seconds: int):
        self.max_queue_size = max_queue_size
        self.queue_timeout_seconds = queue_timeout_seconds
        self._condition = asyncio.Condition()
        self._wait_queue: Deque[str] = deque()
        self._active_request_id: Optional[str] = None

    async def run(
        self,
        *,
        request_id: str,
        work: Callable[[], Awaitable[T]],
        is_disconnected: Optional[Callable[[], Awaitable[bool]]] = None,
    ) -> QueuedRunResult[T]:
        enqueued_at = time.perf_counter()
        queue_position = await self._enqueue(request_id)
        logger.info("请求 %s 已进入队列，当前位置=%s", request_id, queue_position)

        queued_seconds = await self._wait_for_turn(
            request_id=request_id,
            enqueued_at=enqueued_at,
            is_disconnected=is_disconnected,
        )

        logger.info("请求 %s 排队 %.2f 秒后开始执行", request_id, queued_seconds)
        execution_started = time.perf_counter()
        try:
            result = await work()
            execution_seconds = time.perf_counter() - execution_started
            logger.info("请求 %s 执行完成，耗时 %.2f 秒", request_id, execution_seconds)
            return QueuedRunResult(
                result=result,
                queued_seconds=queued_seconds,
                execution_seconds=execution_seconds,
            )
        finally:
            await self._release(request_id)

    async def _enqueue(self, request_id: str) -> int:
        async with self._condition:
            if len(self._wait_queue) >= self.max_queue_size:
                raise QueueFullError("\u8bf7\u6c42\u6392\u961f\u5df2\u6ee1")

            self._wait_queue.append(request_id)
            queue_position = len(self._wait_queue)
            self._condition.notify_all()
            return queue_position

    async def _wait_for_turn(
        self,
        *,
        request_id: str,
        enqueued_at: float,
        is_disconnected: Optional[Callable[[], Awaitable[bool]]],
    ) -> float:
        while True:
            async with self._condition:
                if self._wait_queue and self._wait_queue[0] == request_id and self._active_request_id is None:
                    self._wait_queue.popleft()
                    self._active_request_id = request_id
                    self._condition.notify_all()
                    return time.perf_counter() - enqueued_at

            if is_disconnected is not None and await is_disconnected():
                await self._remove_waiting(request_id)
                raise ClientDisconnectedError("\u5ba2\u6237\u7aef\u5728\u6392\u961f\u9636\u6bb5\u65ad\u5f00")

            waited_seconds = time.perf_counter() - enqueued_at
            remaining = self.queue_timeout_seconds - waited_seconds
            if remaining <= 0:
                await self._remove_waiting(request_id)
                raise QueueTimeoutError("\u8bf7\u6c42\u6392\u961f\u8d85\u65f6")

            try:
                async with self._condition:
                    await asyncio.wait_for(self._condition.wait(), timeout=min(0.25, remaining))
            except asyncio.TimeoutError:
                continue

    async def _remove_waiting(self, request_id: str) -> None:
        async with self._condition:
            try:
                self._wait_queue.remove(request_id)
            except ValueError:
                return
            self._condition.notify_all()

    async def _release(self, request_id: str) -> None:
        async with self._condition:
            if self._active_request_id == request_id:
                self._active_request_id = None
            self._condition.notify_all()

    def snapshot(self) -> Dict[str, Any]:
        return {
            "active_request_id": self._active_request_id,
            "waiting_count": len(self._wait_queue),
            "waiting_request_ids": list(self._wait_queue),
            "max_queue_size": self.max_queue_size,
            "queue_timeout_seconds": self.queue_timeout_seconds,
        }


class CertificateService:
    """负责单个批次请求的查询与飞书回填编排。"""

    def __init__(
        self,
        *,
        feishu_config: Dict[str, str],
        max_workers: int = 3,
        chrome_bin: Optional[str] = None,
        chromedriver_path: Optional[str] = None,
    ):
        self.feishu_client = FeishuTableReader(**feishu_config)
        self.max_workers = max_workers
        self.chrome_bin = chrome_bin
        self.chromedriver_path = chromedriver_path

    def _query_single_person(self, person: Dict[str, str], worker_index: int = 0) -> PersonQueryResult:
        """单个人员查询逻辑，供线程池调用"""
        from pathlib import Path
        record_id = person.get("record_id", "")
        name = person.get("name", "")
        id_number = person.get("id_number", "")

        # 为每个并发 worker 分配独立的 profile 子目录，避免 Chrome 数据目录锁冲突
        project_root = Path(__file__).resolve().parents[1]
        base_data_dir = project_root / ".chrome_data"
        base_data_dir.mkdir(exist_ok=True)
        
        # 每个 worker 使用独立的 profile 目录: profile_0, profile_1, ...
        user_data_dir = base_data_dir / f"profile_{worker_index}"
        user_data_dir.mkdir(exist_ok=True)

        try:
            with CertificateQuery(
                chrome_bin=self.chrome_bin, 
                chromedriver_path=self.chromedriver_path,
                user_data_dir=str(user_data_dir)
            ) as query:
                return query.query_person(record_id, name, id_number)
        except Exception as e:
            logger.error(f"查询人员 {name} 失败: {e}")
            return PersonQueryResult(
                record_id=record_id,
                name=name,
                id_number=id_number,
                status="fail_other",
                error=str(e),
            )

    def run_batch(
        self, 
        people: List[Dict[str, str]], 
        progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> List[PersonQueryResult]:
        """并发执行批量查询，并保持输入顺序"""
        total = len(people)
        logger.info(f"开启并发查询，总人数: {total}, 最大并发数: {self.max_workers}")
        
        results: List[PersonQueryResult] = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交任务，worker_index 取模确保复用有限的 profile 目录
            futures = [
                executor.submit(self._query_single_person, p, i % self.max_workers) 
                for i, p in enumerate(people)
            ]
            
            for i, future in enumerate(futures):
                result = future.result()
                results.append(result)
                if progress_callback:
                    progress_callback(i + 1, total)
                logger.info(f"[{i+1}/{total}] 完成查询: {result.name} - {result.status}")
                
        return results

    def writeback_to_feishu(
        self, 
        results: List[PersonQueryResult], 
        field_mapping: Dict[str, Dict[str, str]]
    ) -> Dict[str, int]:
        """将结果回填至飞书"""
        if not self.feishu_client:
            raise RuntimeError("未配置飞书客户端，无法回填")

        self.feishu_client.ensure_token()
        
        success_count = 0
        failed_count = 0
        
        for result in results:
            if result.status != "success":
                continue
                
            try:
                # 构造更新字段
                fields = self.build_feishu_fields(result, field_mapping)
                if self.feishu_client.update_record(result.record_id, fields):
                    success_count += 1
                else:
                    failed_count += 1
            except Exception as e:
                logger.error(f"回填人员 {result.name} 失败: {e}")
                failed_count += 1
                
        return {"success": success_count, "failed": failed_count}

    def run_batch_query(self, people: List[Dict[str, str]]) -> List[PersonQueryResult]:
        """使用单个浏览器会话执行批量查询，便于统一编排与测试替身。"""
        with CertificateQuery(
            chrome_bin=self.chrome_bin,
            chromedriver_path=self.chromedriver_path,
        ) as query:
            return query.run_batch_query(people)

    def resolve_record_targets(
        self,
        *,
        people: List[Dict[str, str]],
        lookup: Optional[Dict[str, Optional[str]]],
    ) -> tuple[List[str], List[Optional[str]]]:
        resolved_record_ids = [str(person.get("record_id") or "").strip() for person in people]
        errors: List[Optional[str]] = [None] * len(people)

        missing_indices = [index for index, record_id in enumerate(resolved_record_ids) if not record_id]
        if not missing_indices:
            return resolved_record_ids, errors

        if lookup is None:
            for index in missing_indices:
                errors[index] = LOOKUP_REQUIRED_MESSAGE
            return resolved_record_ids, errors

        records = self.feishu_client.list_records(field_names=self.lookup_field_names(lookup))
        index_map = self.build_lookup_index(records=records, lookup=lookup)
        use_name = bool(lookup.get("name_field"))

        for index in missing_indices:
            person = people[index]
            lookup_key = self.lookup_key(
                name=person.get("name", ""),
                id_number=person.get("id_number", ""),
                use_name=use_name,
            )
            matched_record_ids = index_map.get(lookup_key, [])

            if len(matched_record_ids) == 1:
                resolved_record_ids[index] = matched_record_ids[0]
            elif not matched_record_ids:
                errors[index] = LOOKUP_NOT_FOUND_MESSAGE
            else:
                errors[index] = LOOKUP_MULTIPLE_MESSAGE

        return resolved_record_ids, errors

    def process_batch_request(
        self,
        *,
        request_id: str,
        people: List[Dict[str, str]],
        lookup: Optional[Dict[str, Optional[str]]],
        field_mapping: Dict[str, Dict[str, str]],
        debug: bool = False,
    ) -> BatchProcessResult:
        del debug  # 当前接口默认返回调试字段，保留参数仅为兼容既有调用。

        total = len(people)
        if total == 0:
            return BatchProcessResult(
                total=0,
                success=0,
                failed=0,
                results=[],
                query_seconds=0.0,
                writeback_seconds=0.0,
            )

        self.feishu_client.ensure_token()
        resolved_record_ids, lookup_errors = self.resolve_record_targets(people=people, lookup=lookup)

        query_started = time.perf_counter()
        query_results = self.run_batch_query(people)
        query_seconds = time.perf_counter() - query_started

        writeback_started = time.perf_counter()
        response_rows: List[BatchPersonResult] = []
        success_count = 0

        for index, person in enumerate(people):
            query_result = query_results[index] if index < len(query_results) else PersonQueryResult(
                record_id=str(person.get("record_id") or "").strip(),
                name=person.get("name", ""),
                id_number=person.get("id_number", ""),
                status="fail_other",
                error="查询结果缺失",
            )

            record_id = str(
                query_result.record_id or resolved_record_ids[index] or person.get("record_id") or ""
            ).strip() or None
            query_status = QUERY_STATUS_LABELS.get(query_result.status, query_result.status)
            query_error = str(query_result.error).strip() if query_result.error else None
            writeback_error = lookup_errors[index]
            person_success = False

            if query_result.status != "success":
                writeback_error = writeback_error or SKIPPED_WRITEBACK_MESSAGE
            elif writeback_error:
                logger.warning(
                    "请求 %s 记录 %s 无法回填：%s",
                    request_id,
                    record_id or person.get("id_number", ""),
                    writeback_error,
                )
            elif not record_id:
                writeback_error = LOOKUP_REQUIRED_MESSAGE
            else:
                try:
                    fields = self.build_feishu_fields(
                        result=query_result,
                        field_mapping=field_mapping,
                        record_reference=record_id,
                    )
                    if self.feishu_client.update_record(record_id, fields):
                        person_success = True
                        success_count += 1
                    else:
                        writeback_error = WRITEBACK_FAILED_MESSAGE
                except Exception as exc:
                    logger.error("请求 %s 回填记录 %s 失败: %s", request_id, record_id, exc, exc_info=True)
                    writeback_error = str(exc)

            response_rows.append(
                BatchPersonResult(
                    name=query_result.name or person.get("name", ""),
                    id_number=query_result.id_number or person.get("id_number", ""),
                    success=person_success,
                    record_id=record_id,
                    query_status=query_status,
                    query_error=query_error,
                    writeback_error=writeback_error,
                )
            )

        writeback_seconds = time.perf_counter() - writeback_started
        failed_count = total - success_count
        logger.info(
            "请求 %s 批量处理完成: total=%s success=%s failed=%s query=%.2fs writeback=%.2fs",
            request_id,
            total,
            success_count,
            failed_count,
            query_seconds,
            writeback_seconds,
        )
        return BatchProcessResult(
            total=total,
            success=success_count,
            failed=failed_count,
            results=response_rows,
            query_seconds=query_seconds,
            writeback_seconds=writeback_seconds,
        )

    def build_feishu_fields(
        self, 
        result: PersonQueryResult, 
        field_mapping: Dict[str, Dict[str, str]],
        record_reference: str = "",
        save_dir: Optional[str] = None
    ) -> Dict[str, Any]:
        fields = self.build_reset_fields(field_mapping)

        for cert_type, card in result.selected_certificates.items():
            mapping = field_mapping.get(cert_type)
            if mapping is None:
                continue

            expire_text = (card.fields.get(EFFECTIVE_END_FIELD) or "").strip()
            expire_timestamp = self.date_to_timestamp(expire_text)
            if expire_timestamp is not None:
                fields[mapping["expire_field"]] = expire_timestamp

            review_due_text = (card.fields.get(REVIEW_DUE_FIELD) or "").strip()
            review_due_timestamp = self.date_to_timestamp(review_due_text)
            if review_due_timestamp is not None:
                fields[mapping["review_due_field"]] = review_due_timestamp

            review_actual_text = (card.fields.get(REVIEW_ACTUAL_FIELD) or "").strip()
            review_actual_timestamp = self.date_to_timestamp(review_actual_text)
            if review_actual_timestamp is not None:
                fields[mapping["review_actual_field"]] = review_actual_timestamp

            if not card.screenshot_bytes:
                cert_label = CERTIFICATE_TYPE_LABELS.get(cert_type, cert_type)
                raise RuntimeError(f"{cert_label}截图缺失，无法上传附件")

            filename = self.build_certificate_filename(
                result=result,
                record_reference=record_reference or result.record_id or "",
                cert_type=cert_type,
                card=card,
            )

            # 如果提供了 save_dir，将截图保存到本地
            if save_dir:
                try:
                    p = Path(save_dir)
                    p.mkdir(parents=True, exist_ok=True)
                    local_path = p / filename
                    with open(local_path, "wb") as f:
                        f.write(card.screenshot_bytes)
                    logger.info(f"已保存证书截图到本地: {local_path}")
                except Exception as e:
                    logger.error(f"保存本地截图失败: {e}")

            file_token = self.feishu_client.upload_image(filename=filename, content=card.screenshot_bytes)
            fields[mapping["attachment_field"]] = self.feishu_client.build_attachment_field(
                file_token=file_token,
                filename=filename,
                size=len(card.screenshot_bytes),
                mime_type=card.screenshot_mime or "image/jpeg",
            )

        return fields

    @staticmethod
    def build_reset_fields(field_mapping: Dict[str, Dict[str, str]]) -> Dict[str, Any]:
        fields: Dict[str, Any] = {}
        for mapping in field_mapping.values():
            fields[mapping["expire_field"]] = None
            fields[mapping["review_due_field"]] = None
            fields[mapping["review_actual_field"]] = None
            fields[mapping["attachment_field"]] = []
        return fields

    @staticmethod
    def build_certificate_filename(
        *,
        result: PersonQueryResult,
        record_reference: str,
        cert_type: str,
        card: ExtractedCertificateCard,
    ) -> str:
        def _sanitize_filename_part(value: str) -> str:
            return (
                str(value or "")
                .strip()
                .replace("/", "_")
                .replace("\\", "_")
                .replace(" ", "_")
            )

        expire_text = (card.fields.get(EFFECTIVE_END_FIELD) or "").strip() or "\u672a\u77e5\u65e5\u671f"
        raw_reference = record_reference or result.record_id or result.id_number or result.name or "\u672a\u77e5\u6807\u8bc6"
        safe_name = _sanitize_filename_part(result.name)
        safe_reference = _sanitize_filename_part(raw_reference)
        cert_label = CERTIFICATE_TYPE_LABELS.get(cert_type, cert_type)
        parts = []
        if safe_name:
            parts.append(safe_name)
        if safe_reference and safe_reference != safe_name:
            parts.append(safe_reference)
        parts.extend([cert_label, expire_text])
        return "_".join(parts) + ".jpg"

    @staticmethod
    def lookup_field_names(lookup: Dict[str, Optional[str]]) -> List[str]:
        field_names = [lookup["id_number_field"]]
        name_field = lookup.get("name_field")
        if name_field:
            field_names.append(name_field)
        return field_names

    @classmethod
    def build_lookup_index(
        cls,
        *,
        records: List[Any],
        lookup: Dict[str, Optional[str]],
    ) -> Dict[tuple[str, str], List[str]]:
        index: Dict[tuple[str, str], List[str]] = defaultdict(list)
        id_number_field = lookup["id_number_field"]
        name_field = lookup.get("name_field")

        for record in records:
            record_id, fields = cls.extract_record_payload(record)
            if not record_id:
                continue

            id_number = cls.normalize_lookup_value(fields.get(id_number_field), is_id_number=True)
            if not id_number:
                continue

            name = ""
            if name_field:
                name = cls.normalize_lookup_value(fields.get(name_field), is_id_number=False)
                if not name:
                    continue

            index[(id_number, name)].append(record_id)

        return dict(index)

    @staticmethod
    def extract_record_payload(record: Any) -> tuple[str, Dict[str, Any]]:
        if isinstance(record, dict):
            return (
                str(record.get("record_id") or "").strip(),
                dict(record.get("fields") or {}),
            )
        return (
            str(getattr(record, "record_id", "") or "").strip(),
            dict(getattr(record, "fields", None) or {}),
        )

    @classmethod
    def lookup_key(cls, *, name: str, id_number: str, use_name: bool) -> tuple[str, str]:
        return (
            cls.normalize_lookup_value(id_number, is_id_number=True),
            cls.normalize_lookup_value(name, is_id_number=False) if use_name else "",
        )

    @staticmethod
    def normalize_lookup_value(value: Any, *, is_id_number: bool) -> str:
        if value is None:
            text = ""
        elif isinstance(value, str):
            text = value
        elif isinstance(value, (int, float, bool)):
            text = str(value)
        elif isinstance(value, dict):
            text = str(value.get("text") or value.get("name") or value.get("value") or "")
        elif isinstance(value, list):
            parts = []
            for item in value:
                if isinstance(item, dict):
                    parts.append(str(item.get("text") or item.get("name") or item.get("value") or ""))
                else:
                    parts.append(str(item))
            text = " ".join(part for part in parts if part)
        else:
            text = str(value)

        if is_id_number:
            return "".join(text.split()).upper()
        return text.strip()

    @staticmethod
    def date_to_timestamp(date_text: str) -> Optional[int]:
        parsed = CertificateQuery.parse_date(date_text)
        if parsed is None:
            return None
        return int(parsed.timestamp() * 1000)
