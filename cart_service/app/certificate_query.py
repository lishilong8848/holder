import os
import re
import subprocess
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime
from io import BytesIO
from typing import Any, Dict, List, Optional, Tuple

from PIL import Image

# Pillow 10+ 移除了 ANTIALIAS，但 ddddocr 内部仍在使用，需要兼容补丁
if not hasattr(Image, "ANTIALIAS"):
    Image.ANTIALIAS = Image.LANCZOS

import ddddocr
from selenium import webdriver
from selenium.common.exceptions import NoAlertPresentException, TimeoutException, WebDriverException
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


QUERY_URL = "https://cx.mem.gov.cn/special"
ID_CARD_DROPDOWN_XPATH = "//input[contains(@placeholder, '请选择证件类型')]"
ID_CARD_OPTION_XPATH = (
    "//li[contains(@class, 'el-select-dropdown__item')]//span[normalize-space()='身份证']"
)
ID_NUMBER_INPUT_XPATH = "//input[contains(@placeholder, '请输入证件号码')]"
NAME_INPUT_XPATH = "//input[contains(@placeholder, '请输入姓名')]"
CAPTCHA_INPUT_XPATH = "//input[contains(@placeholder, '请输入验证码')]"
CAPTCHA_IMAGE_XPATH = "//img[contains(@class, 'yzm-style-img')]"
QUERY_BUTTON_XPATH = (
    "//button[contains(@class, 'queryBtn') and contains(@class, 'el-button--primary')]"
)
RESULT_TABLE_XPATH = "//table[contains(@class, 'el-descriptions__table')]"

NAME_FIELD = "姓名"
OPERATION_ITEM_FIELD = "操作项目"
EFFECTIVE_END_FIELD = "有效期结束日期"
FIRST_ISSUE_FIELD = "初领日期"

NO_RESULT_TEXT = "没有查询到相关证件信息"
HISTORY_TEXT = "历史数据"

ID_15_RE = re.compile(r"^\d{15}$")
ID_18_RE = re.compile(r"^\d{17}[\dX]$")
ID_CHECKSUM_WEIGHTS = (7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2)
ID_CHECKSUM_CODES = "10X98765432"

CERT_TYPE_BY_OPERATION_ITEM = {
    "高压电工作业": "high_voltage",
    "低压电工作业": "low_voltage",
    "制冷与空调设备运行操作作业": "refrigeration",
    "高处安装、维护、拆除作业": "working_at_height",
    "熔化焊接与热切割作业": "welding",
    "压力焊作业": "welding",
    "钎焊作业": "welding",
}


@dataclass
class ExtractedCertificateCard:
    fields: Dict[str, Any]
    screenshot_bytes: Optional[bytes]
    screenshot_mime: Optional[str] = "image/jpeg"


@dataclass
class PersonQueryResult:
    record_id: str
    name: str
    id_number: str
    status: str
    error: Optional[str]
    records: List[Dict[str, Any]] = field(default_factory=list)
    selected_certificates: Dict[str, ExtractedCertificateCard] = field(default_factory=dict)
    queried_at: str = ""


class CertificateQuery:
    """Synchronous batch certificate query engine backed by Selenium."""

    def __init__(
        self,
        *,
        max_retries: int = 5,
        page_timeout: int = 20,
        query_timeout: int = 12,
        chrome_bin: Optional[str] = None,
        chromedriver_path: Optional[str] = None,
        user_data_dir: Optional[str] = None,
    ):
        self.max_retries = max_retries
        self.page_timeout = page_timeout
        self.query_timeout = query_timeout
        self.chrome_bin = chrome_bin or os.environ.get("CHROME_BIN")
        self.chromedriver_path = chromedriver_path or os.environ.get("CHROMEDRIVER_PATH")
        self.user_data_dir = user_data_dir
        self._closed = False

        self.driver = self._build_driver()
        self.driver.set_page_load_timeout(30)
        self.driver.implicitly_wait(0)
        self.page_wait = WebDriverWait(self.driver, self.page_timeout)
        self.query_wait = WebDriverWait(self.driver, self.query_timeout)
        self.ocr = ddddocr.DdddOcr()
        self._service_process = getattr(getattr(self.driver, "service", None), "process", None)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    def _build_driver(self):
        options = webdriver.ChromeOptions()
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-software-rasterizer")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)
        options.add_argument("--remote-allow-origins=*")
        options.add_argument("--ignore-certificate-errors")

        if self.user_data_dir:
            # 尝试清理可能导致崩溃的锁文件 (Windows 环境常见)
            lock_file = os.path.join(self.user_data_dir, "SingletonLock")
            if os.path.exists(lock_file):
                with suppress(Exception):
                    os.remove(lock_file)
            options.add_argument(f"--user-data-dir={self.user_data_dir}")

        if self.chrome_bin:
            options.binary_location = self.chrome_bin

        service = ChromeService(executable_path=self.chromedriver_path) if self.chromedriver_path else None

        try:
            if service:
                return webdriver.Chrome(service=service, options=options)
            return webdriver.Chrome(options=options)
        except WebDriverException as exc:
            # 记录详细错误信息
            error_msg = f"Chrome 启动失败: {exc}"
            if self.chrome_bin:
                error_msg += f" (路径: {self.chrome_bin})"
            raise RuntimeError(error_msg) from exc

    @classmethod
    def normalize_id_number(cls, id_number: str) -> Tuple[str, Optional[str]]:
        normalized = re.sub(r"\s+", "", (id_number or "")).upper()
        if not normalized:
            return "", "身份证号不能为空"

        if ID_15_RE.fullmatch(normalized):
            return normalized, None

        if not ID_18_RE.fullmatch(normalized):
            return normalized, "身份证号格式错误"

        checksum = sum(int(char) * weight for char, weight in zip(normalized[:17], ID_CHECKSUM_WEIGHTS))
        expected = ID_CHECKSUM_CODES[checksum % 11]
        if normalized[-1] != expected:
            return normalized, "身份证号校验位错误"

        return normalized, None

    @staticmethod
    def _now_text() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def classify_certificate_type(operation_item: str) -> Optional[str]:
        return CERT_TYPE_BY_OPERATION_ITEM.get((operation_item or "").strip())

    @staticmethod
    def parse_date(date_text: str) -> Optional[datetime]:
        value = (date_text or "").strip()
        if not value:
            return None

        for date_format in ("%Y-%m-%d", "%Y/%m/%d"):
            try:
                return datetime.strptime(value, date_format)
            except ValueError:
                continue
        return None

    @classmethod
    def is_certificate_valid(cls, date_text: str) -> Optional[bool]:
        expire_at = cls.parse_date(date_text)
        if expire_at is None:
            return None
        return expire_at.date() >= datetime.now().date()

    def close(self):
        if self._closed:
            return
        self._closed = True

        service_process = self._service_process
        with suppress(Exception):
            self.driver.quit()

        if service_process and service_process.poll() is None:
            with suppress(Exception):
                if os.name == "nt":
                    subprocess.run(
                        ["taskkill", "/F", "/T", "/PID", str(service_process.pid)],
                        capture_output=True,
                        check=False,
                        timeout=5,
                    )
                else:
                    service_process.kill()

    def open_website(self, force_refresh: bool = False):
        try:
            if force_refresh or QUERY_URL not in self.driver.current_url:
                self.driver.get(QUERY_URL)

            self.page_wait.until(EC.presence_of_element_located((By.XPATH, ID_CARD_DROPDOWN_XPATH)))
            self.page_wait.until(EC.presence_of_element_located((By.XPATH, ID_NUMBER_INPUT_XPATH)))
            self.page_wait.until(EC.presence_of_element_located((By.XPATH, NAME_INPUT_XPATH)))
            self.page_wait.until(EC.presence_of_element_located((By.XPATH, CAPTCHA_IMAGE_XPATH)))
            self.page_wait.until(EC.element_to_be_clickable((By.XPATH, QUERY_BUTTON_XPATH)))
        except TimeoutException as exc:
            raise RuntimeError("目标站点首页未能在预期时间内就绪") from exc
        except WebDriverException as exc:
            raise RuntimeError(f"打开目标站点失败: {exc}") from exc

    def _click(self, element):
        try:
            element.click()
        except WebDriverException:
            self.driver.execute_script("arguments[0].click();", element)

    def _set_input_value(self, xpath: str, value: str):
        element = self.page_wait.until(EC.presence_of_element_located((By.XPATH, xpath)))
        element.clear()
        element.send_keys(value)

    def select_id_card_type(self):
        dropdown = self.page_wait.until(EC.element_to_be_clickable((By.XPATH, ID_CARD_DROPDOWN_XPATH)))
        current_value = dropdown.get_attribute("value") or ""
        if "身份证" in current_value:
            return

        self._click(dropdown)
        option = self.page_wait.until(EC.element_to_be_clickable((By.XPATH, ID_CARD_OPTION_XPATH)))
        self._click(option)
        self.page_wait.until(
            lambda _: "身份证"
            in (self.driver.find_element(By.XPATH, ID_CARD_DROPDOWN_XPATH).get_attribute("value") or "")
        )

    def fill_query_form(self, name: str, id_number: str):
        self._set_input_value(ID_NUMBER_INPUT_XPATH, id_number)
        self._set_input_value(NAME_INPUT_XPATH, name)

    def check_id_input_error(self) -> Optional[str]:
        try:
            error_elements = self.driver.find_elements(By.CLASS_NAME, "el-form-item__error")
        except WebDriverException:
            return None

        for element in error_elements:
            with suppress(WebDriverException):
                if not element.is_displayed():
                    continue
                text = element.text.strip()
                if text and "身份证" in text:
                    return text
        return None

    def recognize_and_input_captcha(self) -> Optional[str]:
        captcha_image = self.page_wait.until(EC.presence_of_element_located((By.XPATH, CAPTCHA_IMAGE_XPATH)))
        captcha_bytes = captcha_image.screenshot_as_png
        captcha_text = re.sub(r"[^0-9A-Za-z]", "", self.ocr.classification(captcha_bytes).strip())
        if len(captcha_text) < 4:
            return None

        captcha_input = self.page_wait.until(EC.presence_of_element_located((By.XPATH, CAPTCHA_INPUT_XPATH)))
        captcha_input.clear()
        captcha_input.send_keys(captcha_text)
        return captcha_text

    def click_query_button(self):
        button = self.page_wait.until(EC.element_to_be_clickable((By.XPATH, QUERY_BUTTON_XPATH)))
        self._click(button)

    def _has_no_result_banner(self) -> bool:
        try:
            elements = self.driver.find_elements(By.CLASS_NAME, "nocert-content")
        except WebDriverException:
            return False

        for element in elements:
            with suppress(WebDriverException):
                if element.is_displayed() and NO_RESULT_TEXT in element.text:
                    return True
        return False

    def _has_result_tables(self) -> bool:
        try:
            tables = self.driver.find_elements(By.XPATH, RESULT_TABLE_XPATH)
        except WebDriverException:
            return False
        return any(table.is_displayed() for table in tables)

    def _accept_alert_if_present(self) -> Optional[str]:
        try:
            alert = self.driver.switch_to.alert
        except NoAlertPresentException:
            return None

        alert_text = alert.text
        with suppress(WebDriverException):
            alert.accept()
        return alert_text or "查询被站点弹窗中断"

    def wait_for_query_outcome(self) -> Optional[Tuple[str, Optional[str]]]:
        def _resolve(_driver):
            alert_text = self._accept_alert_if_present()
            if alert_text:
                return ("alert", alert_text)
            if self._has_no_result_banner():
                return ("fail_no_data", None)
            if self._has_result_tables():
                return ("success", None)
            return False

        try:
            return self.query_wait.until(_resolve)
        except TimeoutException:
            return None

    def _is_history_table(self, table) -> bool:
        try:
            return bool(
                self.driver.execute_script(
                    """
                    const currentTable = arguments[0];
                    const dividers = document.querySelectorAll('.el-divider__text');
                    for (const divider of dividers) {
                        if (divider.textContent.includes(arguments[1])) {
                            if (currentTable.compareDocumentPosition(divider) & Node.DOCUMENT_POSITION_PRECEDING) {
                                return true;
                            }
                        }
                    }
                    return false;
                    """,
                    table,
                    HISTORY_TEXT,
                )
            )
        except WebDriverException:
            return False

    @staticmethod
    def _compress_png_to_jpeg(png_bytes: bytes) -> bytes:
        image = Image.open(BytesIO(png_bytes)).convert("RGB")
        buffer = BytesIO()
        # Pillow 10+ 移除了 ANTIALIAS，改用 Resampling.LANCZOS 或直接使用 Image.LANCZOS
        image.save(buffer, format="JPEG", quality=80, optimize=True)
        return buffer.getvalue()

    def capture_element_screenshot(self, element) -> Optional[bytes]:
        """截取元素完整截图，自动处理超出视口的情况"""
        try:
            # 先滚动到元素位置确保可见
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
            import time
            time.sleep(0.3)  # 等待滚动完成和渲染稳定

            # 获取元素实际尺寸
            elem_height = element.size["height"]
            elem_width = element.size["width"]
            
            # 获取当前视口大小
            viewport = self.driver.get_window_size()
            original_width = viewport["width"]
            original_height = viewport["height"]

            # 如果元素高度超过视口，临时扩大窗口以容纳完整元素
            need_resize = elem_height > original_height - 200 or elem_width > original_width - 100
            if need_resize:
                new_height = max(original_height, elem_height + 300)
                new_width = max(original_width, elem_width + 100)
                self.driver.set_window_size(new_width, new_height)
                time.sleep(0.3)
                # 重新滚动（窗口大小变了之后位置可能偏移）
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
                time.sleep(0.2)

            png_bytes = element.screenshot_as_png

            # 恢复窗口大小
            if need_resize:
                self.driver.set_window_size(original_width, original_height)

            return self._compress_png_to_jpeg(png_bytes)
        except Exception:
            return None

    def extract_certificate_cards(self) -> List[ExtractedCertificateCard]:
        tables = self.driver.find_elements(By.XPATH, RESULT_TABLE_XPATH)
        extracted: List[ExtractedCertificateCard] = []
        signatures = set()

        for table in tables:
            if self._is_history_table(table):
                break

            row_data: Dict[str, Any] = {}
            rows = table.find_elements(By.XPATH, ".//tr[contains(@class, 'el-descriptions-row')]")
            for row in rows:
                headers = row.find_elements(By.TAG_NAME, "th")
                cells = row.find_elements(By.TAG_NAME, "td")
                for index, header in enumerate(headers):
                    if index >= len(cells):
                        continue
                    label = header.text.strip()
                    if label:
                        row_data[label] = cells[index].text.strip()

            if not row_data:
                continue

            signature = (
                row_data.get(NAME_FIELD, ""),
                row_data.get(OPERATION_ITEM_FIELD, ""),
                row_data.get(EFFECTIVE_END_FIELD, ""),
                row_data.get(FIRST_ISSUE_FIELD, ""),
            )
            if signature in signatures:
                continue
            signatures.add(signature)

            extracted.append(
                ExtractedCertificateCard(
                    fields=row_data,
                    screenshot_bytes=self.capture_element_screenshot(table),
                )
            )

        return extracted

    @classmethod
    def select_primary_certificates(
        cls, cards: List[ExtractedCertificateCard]
    ) -> Dict[str, ExtractedCertificateCard]:
        selected: Dict[str, ExtractedCertificateCard] = {}

        for card in cards:
            cert_type = cls.classify_certificate_type(card.fields.get(OPERATION_ITEM_FIELD, ""))
            if not cert_type:
                continue

            current = selected.get(cert_type)
            if current is None:
                selected[cert_type] = card
                continue

            current_expire = cls.parse_date(current.fields.get(EFFECTIVE_END_FIELD, ""))
            candidate_expire = cls.parse_date(card.fields.get(EFFECTIVE_END_FIELD, ""))
            if candidate_expire and current_expire:
                if candidate_expire > current_expire:
                    selected[cert_type] = card
            elif candidate_expire and not current_expire:
                selected[cert_type] = card

        return selected

    def _build_result(
        self,
        *,
        record_id: str,
        name: str,
        id_number: str,
        status: str,
        error: Optional[str],
        cards: Optional[List[ExtractedCertificateCard]] = None,
    ) -> PersonQueryResult:
        cards = cards or []
        return PersonQueryResult(
            record_id=record_id,
            name=name,
            id_number=id_number,
            status=status,
            error=error,
            records=[card.fields for card in cards],
            selected_certificates=self.select_primary_certificates(cards),
            queried_at=self._now_text(),
        )

    def query_person(self, record_id: str, name: str, id_number: str) -> PersonQueryResult:
        normalized_id, local_error = self.normalize_id_number(id_number)
        if local_error:
            return self._build_result(
                record_id=record_id,
                name=name,
                id_number=normalized_id,
                status="fail_id",
                error=local_error,
            )

        last_error = "查询失败"
        for _attempt in range(1, self.max_retries + 1):
            try:
                self.open_website(force_refresh=True)
                self.select_id_card_type()
                self.fill_query_form(name, normalized_id)

                page_error = self.check_id_input_error()
                if page_error:
                    return self._build_result(
                        record_id=record_id,
                        name=name,
                        id_number=normalized_id,
                        status="fail_id",
                        error=page_error,
                    )

                captcha_text = self.recognize_and_input_captcha()
                if not captcha_text:
                    last_error = "验证码识别失败"
                    continue

                self.click_query_button()
                outcome = self.wait_for_query_outcome()
                if not outcome:
                    last_error = "查询超时，未检测到结果页面"
                    continue

                outcome_status, outcome_error = outcome
                if outcome_status == "alert":
                    last_error = outcome_error or "查询被站点弹窗中断"
                    continue

                if outcome_status == "fail_no_data":
                    return self._build_result(
                        record_id=record_id,
                        name=name,
                        id_number=normalized_id,
                        status="fail_no_data",
                        error=NO_RESULT_TEXT,
                    )

                cards = self.extract_certificate_cards()
                if cards:
                    return self._build_result(
                        record_id=record_id,
                        name=name,
                        id_number=normalized_id,
                        status="success",
                        error=None,
                        cards=cards,
                    )

                last_error = "结果页已打开，但未提取到有效表格数据"
            except Exception as exc:
                last_error = f"查询过程异常: {exc}"

        return self._build_result(
            record_id=record_id,
            name=name,
            id_number=normalized_id,
            status="fail_other",
            error=last_error,
        )

    def run_batch_query(self, people_list: List[Dict[str, str]]) -> List[PersonQueryResult]:
        self.open_website(force_refresh=True)
        results: List[PersonQueryResult] = []
        for person in people_list:
            results.append(
                self.query_person(
                    record_id=(person.get("record_id") or "").strip(),
                    name=(person.get("name") or "").strip(),
                    id_number=person.get("id_number") or "",
                )
            )
        return results


if __name__ == "__main__":
    sample_people = [
        {"record_id": "rec-1", "name": "李世龙", "id_number": "13012620001028361X"},
        {"record_id": "rec-2", "name": "测试错误ID", "id_number": "123456"},
    ]

    with CertificateQuery() as query:
        data = query.run_batch_query(sample_people)
        for item in data:
            print(item.record_id, item.status, len(item.records))
