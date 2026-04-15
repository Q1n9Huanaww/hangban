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

### Go 服务

```bash
go build ./...
go run .
```

## 注意

- 仓库已通过 `.gitignore` 屏蔽会话文件、日志、抓取结果和本地临时文件。
- 提交前请确认未包含任何密钥、Cookie、账号信息。
