# hangban

航空数据采集与 API 交付示例项目。

## 当前内容

- `adsb_trace_fetch.py`: ADS-B trace 数据拉取与结构化输出脚本
- `main.go`, `scraper.go`, `models.go`: Go API 服务示例

## 快速开始

### Python 脚本

```bash
python adsb_trace_fetch.py --icao 3c4598 --mode full --output out_trace.json
```

### Apify Actor

- 已包含 `Dockerfile`、`.actor/actor.json`、`.actor/input_schema.json`、`actor_main.py`
- 在 Apify Source 里绑定 Git 仓库后直接 `Build now` 即可
- 运行输入示例：

```json
{
  "icao": "3c4598",
  "mode": "full",
  "includePoints": false,
  "warmup": true
}
```

输出将包含增强字段：

- `latest.flight_phase`: `ground` / `climb` / `cruise` / `descent`
- `latest.is_anomaly`, `latest.anomaly_type`: 异常标记与类型列表
- `latest.freshness_sec`: 当前点鲜活度（秒）
- `latest.confidence`, `latest.confidence_level`: 质量评分
- `summary`: 轨迹摘要（时长、距离、速度均值、爬升/下降累计等）

### Go 服务

```bash
go build ./...
go run .
```

## 注意

- 仓库已通过 `.gitignore` 屏蔽会话文件、日志、抓取结果和本地临时文件。
- 提交前请确认未包含任何密钥、Cookie、账号信息。
