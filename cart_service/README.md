# Certificate Query Writeback API

同步查询应急管理部证书信息，并把结果直接回填到飞书多维表。

## Features

- 仅保留 `GET /healthz` 和 `POST /api/v1/query/batch`
- 单次最多查询 `20` 人，按输入顺序串行执行
- 请求里显式传入飞书配置和字段映射
- `feishu.app_token` 同时兼容 Base token 和 Wiki node token
- 查询成功时回填证书到期日期、应复审日期、实际复审日期和证书截图附件字段
- 接口响应只返回每个人是否端到端成功
- 可选 `debug=true` 时，额外返回 `query_status`、`query_error`、`writeback_error`

## Run

### Local

```bash
pip install -r requirements.txt
python -m app.server
```

默认监听端口 `58000`。

### Docker

```bash
docker build -t cert-service .
docker run -d -p 58000:58000 --name cert-service cert-service
```

## API

### Health Check

```http
GET /healthz
```

响应示例：

```json
{
  "status": "ok"
}
```

### Batch Query And Writeback

```http
POST /api/v1/query/batch
Content-Type: application/json
```

请求体：

```json
{
  "feishu": {
    "app_id": "cli_xxx",
    "app_secret": "secret_xxx",
    "app_token": "base_xxx 或 wiki_xxx",
    "table_id": "tbl_xxx"
  },
  "debug": true,
  "field_mapping": {
    "high_voltage": {
      "expire_field": "高压证-到期日期",
      "review_due_field": "高压证-应复审日期",
      "review_actual_field": "高压证-实际复审日期",
      "attachment_field": "高压证-截图"
    },
    "low_voltage": {
      "expire_field": "低压证-到期日期",
      "review_due_field": "低压证-应复审日期",
      "review_actual_field": "低压证-实际复审日期",
      "attachment_field": "低压证-截图"
    }
  },
  "people": [
    {
      "record_id": "recxxxxxxxx",
      "name": "李世龙",
      "id_number": "13012620001028361X"
    },
    {
      "record_id": "recyyyyyyyy",
      "name": "测试错误ID",
      "id_number": "123456"
    }
  ]
}
```

响应体：

```json
{
  "total": 2,
  "success": 1,
  "failed": 1,
  "results": [
    {
      "record_id": "recxxxxxxxx",
      "success": true,
      "query_status": "success"
    },
    {
      "record_id": "recyyyyyyyy",
      "success": false,
      "query_status": "fail_no_data",
      "query_error": "没有查询到相关证件信息"
    }
  ]
}
```

## Validation

- `people` 不能为空
- 每条记录都必须提供非空的 `record_id`、`name`、`id_number`
- `field_mapping` 至少要提供一个证书类型映射
- 单次批量最多 `20` 人
- `success=true` 表示“查询成功且飞书写回成功”，或该人查到的证书都不在本次传入的字段映射范围内
