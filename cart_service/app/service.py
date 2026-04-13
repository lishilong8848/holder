import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Any, Callable

from .certificate_query import CertificateQuery, PersonQueryResult
from .feishu_reader import FeishuTableReader

logger = logging.getLogger(__name__)

# 证书类型到飞书字段的通用映射逻辑
CERT_MAPPING = {
    "high_voltage": "高压证",
    "low_voltage": "低压证",
    "refrigeration": "制冷证",
    "working_at_height": "登高证",
}

class CertificateService:
    """
    证书查询业务服务类
    处理并发调度、结果解析及飞书回填
    """

    def __init__(
        self,
        feishu_config: Optional[Dict[str, str]] = None,
        max_workers: int = 3,
        chrome_bin: Optional[str] = None,
        chromedriver_path: Optional[str] = None,
    ):
        self.feishu_client = FeishuTableReader(**feishu_config) if feishu_config else None
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

    def build_feishu_fields(
        self, 
        result: PersonQueryResult, 
        field_mapping: Dict[str, Dict[str, str]]
    ) -> Dict[str, Any]:
        """根据映射规则构造飞书更新字典"""
        # 初始化重置字段
        fields: Dict[str, Any] = {}
        for mapping in field_mapping.values():
            fields[mapping["expire_field"]] = None
            fields[mapping["review_due_field"]] = None
            fields[mapping["review_actual_field"]] = None
            fields[mapping["attachment_field"]] = []

        # 遍历查询到的证书
        for cert_type, card in result.selected_certificates.items():
            mapping = field_mapping.get(cert_type)
            if mapping is None:
                continue

            # 到期日期
            expire_text = (card.fields.get("有效期结束日期") or "").strip()
            expire_ts = self._date_to_timestamp(expire_text)
            if expire_ts:
                fields[mapping["expire_field"]] = expire_ts

            # 复审日期 (如果有)
            review_due_text = (card.fields.get("应复审日期") or "").strip()
            review_due_ts = self._date_to_timestamp(review_due_text)
            if review_due_ts:
                fields[mapping["review_due_field"]] = review_due_ts

            review_actual_text = (card.fields.get("实际复审日期") or "").strip()
            review_actual_ts = self._date_to_timestamp(review_actual_text)
            if review_actual_ts:
                fields[mapping["review_actual_field"]] = review_actual_ts

            # 上传截图
            if card.screenshot_bytes and self.feishu_client:
                filename = f"{result.record_id.replace('/', '_')}_{cert_type}_{expire_text or 'unknown'}.jpg"
                file_token = self.feishu_client.upload_image(filename=filename, content=card.screenshot_bytes)
                fields[mapping["attachment_field"]] = self.feishu_client.build_attachment_field(
                    file_token=file_token,
                    filename=filename,
                    size=len(card.screenshot_bytes),
                    mime_type="image/jpeg",
                )

        return fields

    @staticmethod
    def _date_to_timestamp(date_text: str) -> Optional[int]:
        if not date_text:
            return None
        expire_at = CertificateQuery.parse_date(date_text)
        if expire_at is None:
            return None
        return int(expire_at.timestamp() * 1000)
