# 证书查询回填接口

该服务会访问目标网站查询应急管理类证书信息，并将识别出的证书字段回填到飞书多维表。

## 运行模型

- 对外接口仅保留 `GET /healthz` 和 `POST /api/v1/query/batch`
- 采用单进程、单执行槽模式
- 所有请求按 FIFO 规则进入内存队列
- 同一时刻只执行 1 个批次
- `MAX_QUEUE_SIZE` 控制排队容量
- `QUEUE_TIMEOUT_SECONDS` 控制排队超时时间
- 单次批量上限固定为 `20`
- 旧调用方如果仍传 `concurrency`，服务端会忽略

## 启动方式

### 本地启动

```bash
pip install -r requirements.txt
python -m app.server
```

### Docker 启动

```bash
docker build -t cert-service .
docker run -d -p 58000:58000 --name cert-service cert-service
```

默认监听端口为 `58000`。

## 环境变量

详见 [`.env.example`](./.env.example)。

常用变量：

- `PORT`
- `MAX_QUEUE_SIZE`
- `QUEUE_TIMEOUT_SECONDS`
- `CHROME_BIN`
- `CHROMEDRIVER_PATH`
- 可选的默认飞书配置 `FEISHU_*`
- 群消息模式下：
  - `FEISHU_TABLE_ID`：施工单源表
  - `FEISHU_TARGET_TABLE_ID`：证书结果回填表

## 接口说明

### 健康检查

```http
GET /healthz
```

返回示例：

```json
{
  "status": "ok"
}
```

### 批量查询并回填

```http
POST /api/v1/query/batch
Content-Type: application/json
```

请求体示例：

```json
{
  "feishu": {
    "app_id": "cli_xxx",
    "app_secret": "secret_xxx",
    "app_token": "base_or_wiki_xxx",
    "table_id": "tbl_xxx"
  },
  "lookup": {
    "id_number_field": "身份证号",
    "name_field": "姓名"
  },
  "field_mapping": {
    "high_voltage": {
      "expire_field": "高压证-到期日期",
      "review_due_field": "高压证-应复审日期",
      "review_actual_field": "高压证-实际复审日期",
      "attachment_field": "高压证"
    }
  },
  "people": [
    {
      "name": "李世龙",
      "id_number": "130126200000000"
    }
  ]
}
```

`people[].record_id` 为可选字段：

- 如果传了 `record_id`，服务端直接更新该记录
- 如果没有传 `record_id`，服务端会按 `lookup.id_number_field` 查找记录
- 如果同时配置了 `lookup.name_field`，则会按“姓名 + 身份证号”联合匹配
- 如果没有找到匹配记录，或匹配到多条记录，该人员会返回 `success=false`

成功响应示例：

```json
{
  "total": 1,
  "success": 1,
  "failed": 0,
  "results": [
    {
      "name": "李世龙",
      "id_number": "130126200000000",
      "record_id": "rec_xxx",
      "success": true
    }
  ]
}
```

接口会默认返回调试字段，响应示例：

```json
{
  "total": 2,
  "success": 1,
  "failed": 1,
  "results": [
    {
      "name": "李世龙",
      "id_number": "130126200000000",
      "record_id": "rec_xxx",
      "success": true,
      "query_status": "查询成功"
    },
    {
      "name": "范邵桦",
      "id_number": "320601199203000000",
      "success": false,
      "query_status": "未查询到证件信息",
      "query_error": "没有查询到相关证件信息",
      "writeback_error": "查询未成功，跳过回填"
    }
  ]
}
```

## 校验与排队规则

- `people` 不能为空
- 每个 `people` 成员都必须包含非空的 `name` 和 `id_number`
- 如果任意成员未传 `record_id`，则必须提供 `lookup.id_number_field`
- `field_mapping` 至少要配置一种证书类型
- 单次批量上限为 `20`
- 队列满时返回 `429`
- 排队超时返回 `503`
- `success=true` 表示“查询成功且飞书回填成功”

## 手工联调工具

- 示例请求体：[`payload.json`](./payload.json)
- 本地私有请求体建议保存为 `payload.local.json`
- 手工请求脚本：[`tests/manual_batch_request.py`](./tests/manual_batch_request.py)
- 飞书 token 检查脚本：[`scripts/check_feishu_token.py`](./scripts/check_feishu_token.py)
