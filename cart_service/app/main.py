"""
飞书多维表格与证书查询整合模块
1. 从飞书读取人员信息
2. 封装为Person对象
3. 调用证书查询功能
4. 将查询结果回填到Person对象
"""

import json
import os
from dataclasses import dataclass, field, asdict
from typing import List, Optional, Dict, Any, Callable
from datetime import datetime

# 导入飞书读取模块和证书查询模块
# 导入飞书读取模块和证书查询模块
from .feishu_reader import FeishuTableReader
from .certificate_query import CertificateQuery


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

    # 基本信息（从飞书读取）
    姓名: str = ""
    身份证号: str = ""
    员工工号: str = ""
    岗位: str = ""
    公司名称: str = ""
    用工性质: str = ""

    # 飞书表格中的证书状态
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

    # 查询结果（从网站查询后填入）
    查询状态: str = ""  # success, fail_id, fail_no_data, fail_other
    查询时间: str = ""
    查询结果: List[Dict] = field(default_factory=list)
    失败原因: str = ""

    # 原始飞书记录ID
    source_id: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return asdict(self)

    def to_json(self) -> str:
        """转换为JSON字符串"""
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2)


class IntegratedCertificateSystem:
    """整合的证书查询系统"""

    def __init__(
        self,
        app_id: str = None,
        app_secret: str = None,
        app_token: str = None,
        table_id: str = None,
    ):
        """
        初始化整合系统

        Args:
            app_id: 飞书应用ID（可选，默认使用feishu_reader中的配置）
            app_secret: 飞书应用密钥（可选）
            app_token: 多维表格Token（可选）
            table_id: 表格ID（可选）
        """
        # 创建飞书读取器，如果有传入参数则使用传入的参数
        if all([app_id, app_secret, app_token, table_id]):
            self.feishu_reader = FeishuTableReader(
                app_id=app_id,
                app_secret=app_secret,
                app_token=app_token,
                table_id=table_id,
            )
        else:
            self.feishu_reader = FeishuTableReader()

        self.certificate_query = None  # 延迟初始化
        self.people: List[Person] = []

    def load_from_feishu(self) -> List[Person]:
        """从飞书多维表格加载人员信息"""
        print("\n========== 第一步：从飞书加载人员信息 ==========")

        # 获取所有记录
        records = self.feishu_reader.read_records()
        if not records:
            print("无法读取飞书表格数据")
            return []

        # 转换为Person对象
        for record in records:
            fields = record.fields
            person = Person(
                姓名=str(fields.get("姓名", "")),
                身份证号=str(fields.get("身份证号", "")),
                员工工号=str(fields.get("员工工号", "")),
                岗位=str(fields.get("岗位", "")),
                公司名称=str(fields.get("公司名称", "")),
                用工性质=str(fields.get("用工性质", "")),
                高压证_是否有效=str(fields.get("高压证-是否有效", "")),
                低压证_是否有效=str(fields.get("低压证-是否有效", "")),
                制冷证_是否有效=str(fields.get("制冷证-是否有效", "")),
                登高证_是否有效=str(fields.get("登高证-是否有效", "")),
                证书是否合规=str(fields.get("证书是否合规", "")),
                缺少证书=str(fields.get("缺少证书", "") or ""),
                上岗证件要求=str(fields.get("上岗证件要求", "")),
                source_id=record.record_id,  # 保存record_id用于更新
            )
            self.people.append(person)

        print(f"成功加载 {len(self.people)} 条人员信息")
        return self.people

    def query_certificates(self, max_count: int = None):
        """查询证书信息"""
        if not self.people:
            print("请先调用 load_from_feishu() 加载人员信息")
            return

        print("\n========== 第二步：查询证书信息 ==========")

        # 构建查询列表
        people_to_query = self.people[:max_count] if max_count else self.people
        people_list = [
            {"id_number": p.身份证号, "name": p.姓名}
            for p in people_to_query
            if p.身份证号  # 只查询有身份证号的
        ]

        print(f"准备查询 {len(people_list)} 人")

        # 初始化查询器
        self.certificate_query = CertificateQuery()
        result_file = None

        try:
            # 执行查询并获取结果文件路径
            result_file, excel_file = self.certificate_query.run_batch_with_results(
                people_list
            )
            self.last_excel_file = excel_file  # 保存文件名以便返回
        except Exception as e:
            print(f"查询过程发生异常: {e}")
        finally:
            # 确保浏览器被关闭
            if self.certificate_query:
                try:
                    self.certificate_query.close()
                except:
                    pass

        # 从临时文件读取结果并回填到Person对象
        if result_file and os.path.exists(result_file):
            import json

            results = {}
            with open(result_file, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        record = json.loads(line)
                        results[record["id_number"]] = record

            for person in people_to_query:
                if person.身份证号 in results:
                    result = results[person.身份证号]
                    person.查询状态 = result.get("status", "")
                    person.查询时间 = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    person.查询结果 = result.get("data", [])
                    person.失败原因 = result.get("error", "")

                    # 解析查询结果，填充证书状态
                    if person.查询状态 == "success" and person.查询结果:
                        self._fill_certificate_status(person)

            print(f"\n查询完成，结果已从临时文件读取并回填")
            # 清理：结果已读取，不再需要内存中的results
            del results
        else:
            print(f"\n查询完成，未找到结果文件")

    def _fill_certificate_status(self, person: Person):
        """
        解析查询结果，填充证书到期日期和是否有效状态

        映射关系：
        - "低压电工作业" -> "低压证"
        - "高压电工作业" -> "高压证"
        - "制冷与空调设备运行操作作业" -> "制冷证"
        - "高处安装、维护、拆除作业" -> "登高证"
        """
        today = datetime.now().date()

        # 操作项目到证书类型的映射
        cert_mapping = {
            "低压电工作业": ("低压证_到期日期", "低压证_是否有效"),
            "高压电工作业": ("高压证_到期日期", "高压证_是否有效"),
            "制冷与空调设备运行操作作业": ("制冷证_到期日期", "制冷证_是否有效"),
            "高处安装、维护、拆除作业": ("登高证_到期日期", "登高证_是否有效"),
        }

        # 遍历查询结果
        for cert_data in person.查询结果:
            操作项目 = cert_data.get("操作项目", "")
            有效期结束日期 = cert_data.get("有效期结束日期", "")

            # 查找对应的证书类型
            if 操作项目 in cert_mapping:
                到期日期字段, 是否有效字段 = cert_mapping[操作项目]

                # 设置到期日期
                setattr(person, 到期日期字段, 有效期结束日期)

                # 判断是否有效（到期日期是否小于今天）
                if 有效期结束日期:
                    try:
                        expire_date = datetime.strptime(
                            有效期结束日期, "%Y-%m-%d"
                        ).date()
                        if expire_date < today:
                            setattr(person, 是否有效字段, "❌")
                        else:
                            setattr(person, 是否有效字段, "✔️")
                    except ValueError:
                        # 日期格式解析失败，留空
                        pass

    def get_results(self) -> List[Person]:
        """获取所有人员信息（包含查询结果）"""
        return self.people

    def export_to_json(self, filename: str = "查询结果.json"):
        """导出结果到JSON文件"""
        data = [p.to_dict() for p in self.people]
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"结果已导出到: {filename}")
        return filename

    def print_summary(self):
        """打印查询结果摘要"""
        print("\n========== 查询结果摘要 ==========")

        success_count = sum(1 for p in self.people if p.查询状态 == "success")
        fail_id_count = sum(1 for p in self.people if p.查询状态 == "fail_id")
        fail_no_data = sum(1 for p in self.people if p.查询状态 == "fail_no_data")
        fail_other = sum(1 for p in self.people if p.查询状态 == "fail_other")
        not_queried = sum(1 for p in self.people if not p.查询状态)

        print(f"总人数: {len(self.people)}")
        print(f"  ✅ 查询成功: {success_count}")
        print(f"  ⚠️ 身份证错误: {fail_id_count}")
        print(f"  ⚠️ 无证书信息: {fail_no_data}")
        print(f"  ❌ 其他失败: {fail_other}")
        print(f"  ⏳ 未查询: {not_queried}")

    def update_feishu(self):
        """
        将查询结果更新到飞书多维表格

        更新字段：
        - 高压证-到期日期, 高压证-是否有效
        - 低压证-到期日期, 低压证-是否有效
        - 制冷证-到期日期, 制冷证-是否有效
        - 登高证-到期日期, 登高证-是否有效
        """
        print("\n========== 第三步：更新飞书表格 ==========")

        def date_to_timestamp(date_str: str) -> int:
            """将日期字符串转换为时间戳（毫秒）"""
            if not date_str:
                return None
            try:
                from datetime import datetime

                dt = datetime.strptime(date_str, "%Y-%m-%d")
                return int(dt.timestamp() * 1000)
            except:
                return None

        updates = []
        for person in self.people:
            # 只更新查询成功的记录
            if person.查询状态 == "success" and person.source_id:
                # 构建要更新的字段（飞书字段名使用 - 分隔）
                fields = {}

                # 日期字段需要转换为时间戳
                if person.高压证_到期日期:
                    ts = date_to_timestamp(person.高压证_到期日期)
                    if ts:
                        fields["高压证-到期日期"] = ts
                if person.高压证_是否有效:
                    fields["高压证-是否有效"] = person.高压证_是否有效

                if person.低压证_到期日期:
                    ts = date_to_timestamp(person.低压证_到期日期)
                    if ts:
                        fields["低压证-到期日期"] = ts
                if person.低压证_是否有效:
                    fields["低压证-是否有效"] = person.低压证_是否有效

                if person.制冷证_到期日期:
                    ts = date_to_timestamp(person.制冷证_到期日期)
                    if ts:
                        fields["制冷证-到期日期"] = ts
                if person.制冷证_是否有效:
                    fields["制冷证-是否有效"] = person.制冷证_是否有效

                if person.登高证_到期日期:
                    ts = date_to_timestamp(person.登高证_到期日期)
                    if ts:
                        fields["登高证-到期日期"] = ts
                if person.登高证_是否有效:
                    fields["登高证-是否有效"] = person.登高证_是否有效

                if fields:
                    updates.append({"record_id": person.source_id, "fields": fields})
                    print(f"  准备更新: {person.姓名}")

        if updates:
            print(f"\n共 {len(updates)} 条记录待更新...")

            # 分批更新，每批10条
            batch_size = 10
            total_success = 0
            total_failed = 0

            for i in range(0, len(updates), batch_size):
                batch = updates[i : i + batch_size]
                print(f"  正在更新第 {i // batch_size + 1} 批（{len(batch)}条）...")
                result = self.feishu_reader.batch_update_records(batch)
                total_success += result.get("success", 0)
                total_failed += result.get("failed", 0)

                # 清理当前批次内存
                del batch

            # 清理updates列表释放内存
            del updates

            print(f"\n飞书更新完成: 成功 {total_success}, 失败 {total_failed}")
            return {"success": total_success, "failed": total_failed}
        else:
            print("没有需要更新的记录")
            return {"success": 0, "failed": 0}


def run_certificate_query(
    app_id: str,
    app_secret: str,
    app_token: str,
    table_id: str,
    query_all: bool = True,
    query_count: int = None,
    auto_update_feishu: bool = True,
    progress_callback: Callable[[List[Person]], None] = None,
) -> Dict[str, Any]:
    """
    证书查询接口函数

    封装了整个查询流程：从飞书读取 -> 查询证书 -> 更新飞书表格

    Args:
        app_id: 飞书应用ID
        app_secret: 飞书应用密钥
        app_token: 多维表格Token
        table_id: 表格ID
        query_all: 是否查询全部（默认True）。如果query_count有值则此参数不生效
        query_count: 查询人数（可选，指定后query_all不生效）
        auto_update_feishu: 是否自动更新飞书表格（默认True）

    Returns:
        dict: {
            "total": 总人数,
            "queried": 已查询人数,
            "success": 查询成功人数,
            "failed": 查询失败人数,
            "feishu_updated": 飞书更新结果,
            "people": Person对象列表
        }
    """
    print(f"\n{'=' * 50}")
    print("证书查询系统启动")
    print(f"{'=' * 50}")

    # 初始化系统
    system = IntegratedCertificateSystem(
        app_id=app_id, app_secret=app_secret, app_token=app_token, table_id=table_id
    )

    # 1. 从飞书加载人员信息
    people = system.load_from_feishu()
    if progress_callback:
        progress_callback(people)

    if not people:
        return {
            "total": 0,
            "queried": 0,
            "success": 0,
            "failed": 0,
            "feishu_updated": {"success": 0, "failed": 0},
            "people": [],
        }

    # 2. 确定查询数量
    # 如果指定了query_count，则使用query_count
    # 否则，如果query_all为True，则查询全部
    if query_count is not None and query_count > 0:
        max_count = query_count
    elif query_all:
        max_count = None  # None表示全部
    else:
        max_count = 5  # 默认查询5条

    # 3. 执行查询
    system.query_certificates(max_count=max_count)

    # 4. 统计结果
    success_count = sum(1 for p in people if p.查询状态 == "success")
    fail_count = sum(1 for p in people if p.查询状态 and p.查询状态 != "success")
    queried_count = sum(1 for p in people if p.查询状态)

    # 5. 打印摘要
    system.print_summary()

    # 6. 导出结果
    system.export_to_json()

    # 7. 更新飞书表格
    feishu_result = {"success": 0, "failed": 0}
    if auto_update_feishu and success_count > 0:
        feishu_result = system.update_feishu()

    result = {
        "total": len(people),
        "queried": queried_count,
        "success": success_count,
        "failed": fail_count,
        "feishu_updated": feishu_result,
        "excel_file": getattr(system, "last_excel_file", None),  # 导出文件路径
        "people": people,
    }

    print(f"\n{'=' * 50}")
    print("查询完成")
    print(f"{'=' * 50}")

    return result


def main():
    """主函数（交互式）"""
    system = IntegratedCertificateSystem()

    # 1. 从飞书加载人员信息
    people = system.load_from_feishu()

    # 打印前3个人信息预览
    print("\n--- 人员信息预览（前3条）---")
    for p in people[:3]:
        print(f"  {p.姓名} | {p.身份证号} | {p.岗位}")

    # 2. 询问是否开始查询
    print(f"\n共 {len(people)} 人待查询")
    print("是否开始查询证书？")
    print("  1. 查询全部")
    print("  2. 查询前2条（测试）")
    print("  3. 取消")

    choice = input("请选择 (1/2/3): ").strip()

    if choice == "1":
        system.query_certificates()
    elif choice == "2":
        system.query_certificates(max_count=2)
    else:
        print("已取消查询")
        return

    # 3. 打印摘要
    system.print_summary()

    # 4. 导出结果
    system.export_to_json()

    # 5. 询问是否更新飞书表格
    success_count = sum(1 for p in people if p.查询状态 == "success")
    if success_count > 0:
        print(f"\n有 {success_count} 条记录可更新到飞书表格")
        print("是否更新飞书表格？")
        print("  1. 是，更新")
        print("  2. 否，跳过")

        update_choice = input("请选择 (1/2): ").strip()
        if update_choice == "1":
            system.update_feishu()
        else:
            print("已跳过飞书更新")

    # 6. 打印一个完整的Person对象示例
    print("\n--- Person对象示例（第一条已查询的记录）---")
    for p in people:
        if p.查询状态:
            print(p.to_json())
            break


if __name__ == "__main__":
    main()
