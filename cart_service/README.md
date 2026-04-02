# Certificate Query Writeback API

This service queries emergency-management certificates from the target website and writes the selected fields back to a Feishu Bitable row.

## Runtime model

- Public APIs: `GET /healthz` and `POST /api/v1/query/batch`
- Single process, single execution slot
- Requests are handled in FIFO order through an in-memory queue
- Only one batch runs at a time
- `MAX_QUEUE_SIZE` controls queue capacity
- `QUEUE_TIMEOUT_SECONDS` controls queue wait timeout
- Batch size is capped at `20`
- `concurrency` may still be sent by older callers, but the server ignores it

## Start

### Local

```bash
pip install -r requirements.txt
python -m app.server
```

### Docker

```bash
docker build -t cert-service .
docker run -d -p 58000:58000 --name cert-service cert-service
```

The default port is `58000`.

## Environment

See [`.env.example`](./.env.example).

Important variables:

- `PORT`
- `MAX_QUEUE_SIZE`
- `QUEUE_TIMEOUT_SECONDS`
- `CHROME_BIN`
- `CHROMEDRIVER_PATH`
- Optional default Feishu credentials via `FEISHU_*`

## API

### Health check

```http
GET /healthz
```

Response:

```json
{
  "status": "ok"
}
```

### Batch query and writeback

```http
POST /api/v1/query/batch
Content-Type: application/json
```

Request example:

```json
{
  "feishu": {
    "app_id": "cli_xxx",
    "app_secret": "secret_xxx",
    "app_token": "base_or_wiki_xxx",
    "table_id": "tbl_xxx"
  },
  "lookup": {
    "id_number_field": "ID Number",
    "name_field": "Name"
  },
  "debug": true,
  "field_mapping": {
    "high_voltage": {
      "expire_field": "High Voltage Expire Date",
      "review_due_field": "High Voltage Review Due",
      "review_actual_field": "High Voltage Review Actual",
      "attachment_field": "High Voltage Attachment"
    }
  },
  "people": [
    {
      "name": "Alice",
      "id_number": "110101199001011234"
    }
  ]
}
```

`people[].record_id` is optional. If it is omitted, the server looks up the target row in Feishu by:

- `lookup.id_number_field`
- `lookup.name_field` when provided

If no row matches, or more than one row matches, that person returns `success=false`.

Success response:

```json
{
  "total": 1,
  "success": 1,
  "failed": 0,
  "results": [
    {
      "name": "Alice",
      "id_number": "110101199001011234",
      "record_id": "rec_xxx",
      "success": true
    }
  ]
}
```

Debug response example:

```json
{
  "total": 2,
  "success": 1,
  "failed": 1,
  "results": [
    {
      "name": "Alice",
      "id_number": "110101199001011234",
      "record_id": "rec_xxx",
      "success": true,
      "query_status": "success"
    },
    {
      "name": "Bob",
      "id_number": "320601199203020330",
      "success": false,
      "query_status": "fail_no_data",
      "query_error": "no data",
      "writeback_error": "query skipped"
    }
  ]
}
```

## Validation and queue behavior

- `people` cannot be empty
- Each `people` item must include non-empty `name` and `id_number`
- If any `people` item omits `record_id`, `lookup.id_number_field` is required
- `field_mapping` must include at least one certificate type
- Batch size limit is `20`
- Queue full returns `429`
- Queue wait timeout returns `503`
- `success=true` means the certificate query succeeded and the Feishu record update succeeded

## Manual smoke tools

- Example payload: [`payload.json`](./payload.json)
- Local private payload: `payload.local.json`
- Manual API runner: [`tests/manual_batch_request.py`](./tests/manual_batch_request.py)
- Manual Feishu token check: [`scripts/check_feishu_token.py`](./scripts/check_feishu_token.py)
