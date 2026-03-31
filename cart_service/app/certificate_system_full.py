"""
高低压证书自动化查询系统 (单文件集成版)
功能：
1. 从飞书多维表格读取人员信息
2. 自动化登录应急管理部官网查询证书
3. 将查询结果回填并分批更新至飞书
"""

import json
import os
import time
import base64
import shutil
import uuid
import argparse
import subprocess
import atexit
import traceback
from dataclasses import dataclass, field, asdict
from typing import List, Optional, Dict, Any
from datetime import datetime
from io import BytesIO

import pandas as pd
from PIL import Image
import ddddocr
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import lark_oapi as lark
from lark_oapi.api.bitable.v1 import *
from lark_oapi.api.auth.v3 import *

# ==================== 配置默认值 ====================
APP_ID = "cli_a9d1c9fe9d381cee"
APP_SECRET = "PCZSOMI9ITqFWUkqT5TtQcVljP0Sx48y"
APP_TOKEN = "RGLobNV3zaHhlZscp6pcXIzjnxc"
TABLE_ID = "tblGVnroEwBjOeBC"

# ==================== 数据类定义 ====================

@dataclass
class CertificateInfo:
    """证书信息"""
    操作项目: str = ""
    初领日期: str = ""
    有效期开始日期: str = ""
    有效期结束日期: str = ""
    应复审日期: str = ""
    实际复审日期: str = ""
    签发机关: str = ""
    作业类别: str = ""

@dataclass
class Person:
    """人员信息类"""
    姓名: str = ""
    身份证号: str = ""
    员工工号: str = ""
    岗位: str = ""
    公司名称: str = ""
    用工性质: str = ""
    
    高压证_到期日期: str = ""
    高压证_是否有效: str = ""
    低压证_到期日期: str = ""
    低压证_是否有效: str = ""
    制冷证_到期日期: str = ""
    制冷证_是否有效: str = ""
    登高证_到期日期: str = ""
    登高证_是否有效: str = ""
    证书是否合规: str = ""
    缺少证书: str = ""
    上岗证件要求: str = ""
    
    查询状态: str = ""  # success, fail_id, fail_no_data, fail_other
    查询时间: str = ""
    查询结果: List[Dict] = field(default_factory=list)
    失败原因: str = ""
    source_id: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2)

# ==================== 飞书读取模块 ====================

class FeishuTableReader:
    """飞书多维表格读取器"""
    def __init__(self, app_id=APP_ID, app_secret=APP_SECRET, app_token=APP_TOKEN, table_id=TABLE_ID):
        self.app_id = app_id
        self.app_secret = app_secret
        self.app_token = app_token
        self.table_id = table_id
        self.tenant_token = None
        self.token_expire_time = 0
    
    def refresh_token(self):
        try:
            client = lark.Client.builder().build()
            req = (InternalTenantAccessTokenRequest.builder()
                .request_body(InternalTenantAccessTokenRequestBody.builder()
                    .app_id(self.app_id).app_secret(self.app_secret).build())
                .build())
            resp = client.auth.v3.tenant_access_token.internal(req)
            if resp.success():
                data = json.loads(resp.raw.content)
                self.tenant_token = data["tenant_access_token"]
                self.token_expire_time = int(time.time()) + data.get("expire", 7200) - 300
                return True
            return False
        except:
            return False
    
    def ensure_token(self):
        if not self.tenant_token or int(time.time()) >= self.token_expire_time:
            return self.refresh_token()
        return True

    def read_records(self, page_size=100):
        if not self.ensure_token(): return None
        try:
            client = lark.Client.builder().enable_set_token(True).log_level(lark.LogLevel.ERROR).build()
            all_records, page_token = [], None
            while True:
                rb = ListAppTableRecordRequest.builder().app_token(self.app_token).table_id(self.table_id).page_size(page_size)
                if page_token: rb.page_token(page_token)
                req = rb.build()
                opt = lark.RequestOption.builder().tenant_access_token(self.tenant_token).build()
                resp = client.bitable.v1.app_table_record.list(req, opt)
                if resp.success():
                    if resp.data.items: all_records.extend(resp.data.items)
                    if resp.data.has_more: page_token = resp.data.page_token
                    else: break
                else: return None
            return all_records
        except: return None

    def update_record(self, record_id: str, fields: dict) -> bool:
        if not self.ensure_token(): return False
        try:
            client = lark.Client.builder().enable_set_token(True).log_level(lark.LogLevel.ERROR).build()
            req = UpdateAppTableRecordRequest.builder().app_token(self.app_token).table_id(self.table_id).record_id(record_id).request_body(AppTableRecord.builder().fields(fields).build()).build()
            opt = lark.RequestOption.builder().tenant_access_token(self.tenant_token).build()
            resp = client.bitable.v1.app_table_record.update(req, opt)
            return resp.success()
        except: return False

    def batch_update_records(self, updates: list) -> dict:
        success, failed = 0, 0
        for u in updates:
            if self.update_record(u.get("record_id"), u.get("fields", {})): success += 1
            else: failed += 1
        return {"success": success, "failed": failed}

# ==================== 证书查询模块 ====================

class CertificateQuery:
    """证书查询自动化类"""
    def __init__(self, log_callback=None):
        self.log_callback = log_callback
        options = webdriver.ChromeOptions()
        # 移除固定缓存目录以避免锁定问题，使用Selenium默认临时目录
        # self.cache_dir = os.path.join(os.path.dirname(__file__), 'chrome_cache')
        # os.makedirs(self.cache_dir, exist_ok=True)
        # options.add_argument(f'--user-data-dir={self.cache_dir}')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option('excludeSwitches', ['enable-automation'])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument('--headless=new')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        self.driver = webdriver.Chrome(options=options)
        self.wait = WebDriverWait(self.driver, 15)
        self.ocr = ddddocr.DdddOcr(show_ad=False)
        self.cache_dir = None # 显式初始化为None
        atexit.register(self.close)
        self._closed = False

    def close(self):
        """关闭浏览器（确保无论如何都关闭）"""
        if self._closed:
            return
        self._closed = True
        import subprocess
        try:
            if self.cache_dir: # 仅当使用了自定义缓存目录时尝试通过命令行匹配清理
                result = subprocess.run(
                    ['wmic', 'process', 'where', 
                     f"commandline like '%{os.path.basename(self.cache_dir)}%'", 
                     'get', 'processid'],
                    capture_output=True, text=True, timeout=5
                )
                pids = [p.strip() for p in result.stdout.split() if p.strip().isdigit()]
                if pids:
                    for pid in pids:
                        subprocess.run(['taskkill', '/F', '/PID', pid], capture_output=True, timeout=3)
                    print(f"已强制关闭Chrome进程: {pids}")
        except: pass
        try:
            self.driver.quit()
        except: pass

    def open_website(self):
        """打开目标网站"""
        self.driver.get("https://cx.mem.gov.cn/special")
        try:
            self.wait.until(EC.presence_of_element_located((By.XPATH, "//button[contains(@class, 'queryBtn')]")))
            print("网站打开成功")
        except: pass

    def extract_table_data(self):
        """提取表格数据"""
        try:
            time.sleep(2)
            tables = self.driver.find_elements(By.XPATH, "//table[contains(@class, 'el-descriptions__table')]")
            all_data = []
            for t in tables:
                # 检查是否在大数据历史分隔符之后
                is_history = self.driver.execute_script("""
                    var t = arguments[0]; var ds = document.querySelectorAll('.el-divider__text');
                    for (var i=0; i<ds.length; i++) {
                        if (ds[i].textContent.includes('历史数据') && (t.compareDocumentPosition(ds[i]) & 2)) return true;
                    }
                    return false;
                """, t)
                if is_history: break
                
                data = {}
                rows = t.find_elements(By.XPATH, ".//tr[@class='el-descriptions-row']")
                for row in rows:
                    ths = row.find_elements(By.TAG_NAME, "th")
                    tds = row.find_elements(By.TAG_NAME, "td")
                    for i in range(len(ths)):
                        label = ths[i].text.strip()
                        if label: data[label] = tds[i].text.strip()
                if data: all_data.append(data)
            return all_data
        except: return []

    def select_id_card_type(self):
        try:
            dropdown = self.wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@placeholder='请选择证件类型']")))
            dropdown.click()
            time.sleep(1)
            self.wait.until(EC.element_to_be_clickable((By.XPATH, "//li//span[text()='身份证']"))).click()
            return True
        except: return False

    def process_person(self, id_number, name, max_retries=5, skip_select_type=False):
        """处理单个人的查询流程"""
        msg = f"\n>>> 开始查询: {name} ({id_number})"
        print(msg)
        if self.log_callback: self.log_callback(msg)
        try:
            if not skip_select_type:
                if not self.select_id_card_type(): return 'fail_other', {'失败原因': '无法选择证件类型'}
            
            id_in = self.wait.until(EC.presence_of_element_located((By.XPATH, "//input[@placeholder='请输入证件号码']")))
            id_in.clear(); id_in.send_keys(id_number)
            name_in = self.wait.until(EC.presence_of_element_located((By.XPATH, "//input[@placeholder='请输入姓名']")))
            name_in.clear(); name_in.send_keys(name)
            
            # 输入后检查校验错误
            time.sleep(0.5)
            errs = self.driver.find_elements(By.CLASS_NAME, "el-form-item__error")
            for e in errs:
                if e.is_displayed(): return 'fail_id', {'失败原因': e.text.strip()}

            for attempt in range(max_retries):
                print(f"--- 尝试 {attempt+1}/{max_retries} ---")
                captcha_img = self.wait.until(EC.presence_of_element_located((By.XPATH, "//img[@class='yzm-style-img']")))
                img_data = base64.b64decode(captcha_img.get_attribute('src').split(',')[1])
                code = self.ocr.classification(img_data)
                
                c_in = self.driver.find_element(By.XPATH, "//input[@placeholder='请输入验证码']")
                c_in.clear(); c_in.send_keys(code)
                self.driver.find_element(By.XPATH, "//button[contains(@class, 'queryBtn')]").click()
                time.sleep(2)
                
                try:
                    alert = self.driver.switch_to.alert
                    print(f"检测到提示: {alert.text}"); alert.accept()
                    captcha_img.click(); time.sleep(1); continue
                except: pass
                
                if len(self.driver.find_elements(By.CLASS_NAME, "nocert-content")) > 0:
                    return 'fail_no_data', {'失败原因': '没有查询到相关证件信息'}
                
                tables_data = self.extract_table_data()
                if tables_data: return 'success', tables_data
                
            return 'fail_other', {'失败原因': '重试失败或验证码持续错误'}
        except Exception as e: return 'fail_other', {'失败原因': str(e)}

    def run_batch_with_results(self, people_list):
        self.open_website()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        res_file = os.path.join(os.path.dirname(__file__), 'output', f'查询结果_{timestamp}.jsonl')
        os.makedirs(os.path.dirname(res_file), exist_ok=True)
        
        for person in people_list:
            status, data = self.process_person(person['id_number'], person['name'])
            record = {'id_number': person['id_number'], 'name': person['name'], 'status': status, 'data': data if status == 'success' else [], 'error': data.get('失败原因', '') if status != 'success' else ''}
            with open(res_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(record, ensure_ascii=False) + '\n')
            if status in ['success', 'fail_no_data']:
                try: self.driver.find_element(By.XPATH, "//span[contains(text(), '返回')]").click()
                except: self.driver.get("https://cx.mem.gov.cn/special")
        self.close()
        return res_file

# ==================== 整合系统类 ====================

class IntegratedCertificateSystem:
    def __init__(self, log_callback=None, **kwargs):
        self.feishu_reader = FeishuTableReader(**kwargs)
        self.people = []
        self.log_callback = log_callback

    def load_from_feishu(self):
        records = self.feishu_reader.read_records()
        if not records: return []
        for r in records:
            f = r.fields
            self.people.append(Person(姓名=str(f.get("姓名", "")), 身份证号=str(f.get("身份证号", "")), source_id=r.record_id))
        return self.people

    def query_certificates(self, max_count=None):
        subset = self.people[:max_count] if max_count else self.people
        plist = [{"id_number": p.身份证号, "name": p.姓名} for p in subset if p.身份证号]
        query_engine = CertificateQuery(log_callback=self.log_callback)
        try:
            res_file = query_engine.run_batch_with_results(plist)
            if os.path.exists(res_file):
                with open(res_file, 'r', encoding='utf-8') as f:
                    results = {json.loads(line)['id_number']: json.loads(line) for line in f if line.strip()}
                for p in subset:
                    if p.身份证号 in results:
                        res = results[p.身份证号]
                        p.查询状态, p.查询结果, p.失败原因 = res['status'], res['data'], res['error']
                        if p.查询状态 == 'success':
                            self._fill_certificate_status(p)
        finally: query_engine.close()

    def _fill_certificate_status(self, person: Person):
        """解析查询结果，填充证书状态"""
        today = datetime.now().date()
        cert_mapping = {
            "低压电工作业": ("低压证_到期日期", "低压证_是否有效"),
            "高压电工作业": ("高压证_到期日期", "高压证_是否有效"),
            "制冷与空调设备运行操作作业": ("制冷证_到期日期", "制冷证_是否有效"),
            "高处安装、维护、拆除作业": ("登高证_到期日期", "登高证_是否有效"),
        }
        for cert_data in person.查询结果:
            item = cert_data.get("操作项目", "")
            expire_str = cert_data.get("有效期结束日期", "")
            if item in cert_mapping:
                df, sf = cert_mapping[item]
                setattr(person, df, expire_str)
                if expire_str:
                    try:
                        expire_date = datetime.strptime(expire_str, "%Y-%m-%d").date()
                        setattr(person, sf, "✔️" if expire_date >= today else "❌")
                    except: pass


    def print_summary(self):
        print("\n========== 查询结果摘要 ==========")
        s = sum(1 for p in self.people if p.查询状态 == "success")
        fi = sum(1 for p in self.people if p.查询状态 == "fail_id")
        fn = sum(1 for p in self.people if p.查询状态 == "fail_no_data")
        fo = sum(1 for p in self.people if p.查询状态 == "fail_other")
        print(f"总人数: {len(self.people)}\n  ✅ 成功: {s}\n  ⚠️ 证件错误: {fi}\n  ⚠️ 无结果: {fn}\n  ❌ 失败: {fo}")

    def update_feishu(self):
        """分批更新飞书关键日期字段"""
        print("\n========== 第三步：更新飞书表格 ==========")
        def dt_to_ts(ds):
            try: return int(datetime.strptime(ds, "%Y-%m-%d").timestamp() * 1000)
            except: return None
        
        updates = []
        for p in self.people:
            if p.查询状态 == "success" and p.source_id:
                fields = {}
                for lb, key in [("高压证", "高压证"), ("低压证", "低压证"), ("制冷证", "制冷证"), ("登高证", "登高证")]:
                    d, s = getattr(p, f"{key}_到期日期"), getattr(p, f"{key}_是否有效")
                    if d:
                        ts = dt_to_ts(d)
                        if ts: fields[f"{lb}-到期日期"] = ts
                    if s: fields[f"{lb}-是否有效"] = s
                if fields: updates.append({"record_id": p.source_id, "fields": fields})
        
        if updates:
            print(f"共 {len(updates)} 条记录待更新...")
            batch_size = 10
            ts, tf = 0, 0
            for i in range(0, len(updates), batch_size):
                batch = updates[i:i+batch_size]
                res = self.feishu_reader.batch_update_records(batch)
                ts += res.get("success", 0); tf += res.get("failed", 0)
            print(f"飞书更新完成: 成功 {ts}, 失败 {tf}")
            return {"success": ts, "failed": tf}
        return {"success": 0, "failed": 0}

# ==================== 入口接口 ====================

def run_certificate_query(app_id, app_secret, app_token, table_id, query_all=True, query_count=None, auto_update_feishu=True):
    sys = IntegratedCertificateSystem(app_id=app_id, app_secret=app_secret, app_token=app_token, table_id=table_id)
    sys.load_from_feishu()
    sys.query_certificates(max_count=query_count)
    sys.print_summary()
    fu = {"success": 0, "failed": 0}
    if auto_update_feishu: fu = sys.update_feishu()
    return {"total": len(sys.people), "queried": sum(1 for p in sys.people if p.查询状态), "success": sum(1 for p in sys.people if p.查询状态 == "success"), "feishu_updated": fu, "people": sys.people}

def main():
    sys = IntegratedCertificateSystem()
    people = sys.load_from_feishu()
    print(f"\n共 {len(people)} 人待查询")
    choice = input("1. 查询全部\n2. 查询前5条\n3. 取消\n请选择: ").strip()
    if choice == "1": sys.query_certificates()
    elif choice == "2": sys.query_certificates(max_count=5)
    else: return
    sys.print_summary()
    if sum(1 for p in sys.people if p.查询状态 == "success") > 0:
        if input("是否更新飞书？(y/n): ").lower() == 'y': sys.update_feishu()

if __name__ == "__main__":
    main()
