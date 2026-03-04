# databricks-mcp

[![MCP](https://img.shields.io/badge/MCP-Server-blue)](https://modelcontextprotocol.io)
[![Python](https://img.shields.io/badge/Python-3.13+-green)](https://python.org)
[![License](https://img.shields.io/badge/License-MIT-yellow)](../LICENSE)

**其他語言版本: [English](../README.md)**

一個**唯讀**的 MCP (Model Context Protocol) 伺服器，讓 Claude 能夠查詢 Databricks SQL、瀏覽 Metadata、監控 Jobs 和 Pipelines。

## 功能特色

- **SQL 查詢**: 執行 SELECT、SHOW、DESCRIBE 查詢（禁止寫入操作）
- **Metadata 瀏覽**: 列出 catalogs、schemas、tables，搜尋資料表
- **Delta Lake**: 查看資料表歷史、詳情、權限
- **Jobs & Pipelines**: 列出並監控 Databricks Jobs 和 DLT Pipelines
- **Query History**: 瀏覽 SQL 查詢歷史，支援篩選
- **Cluster Metrics**: 從 system tables 監控 CPU、記憶體、網路使用量

## 安裝

### 前置需求

- Python 3.13+
- [uv](https://github.com/astral-sh/uv) 套件管理器
- Databricks workspace 與 SQL Warehouse

### 設定步驟

```bash
# Clone 專案
git clone https://github.com/ChrisChoTW/databricks-mcp.git
cd databricks-mcp

# 安裝相依套件
uv sync

# 建立 .env 檔案
cp .env.example .env
```

### 設定環境變數

編輯 `.env` 填入 Databricks 憑證：

```env
DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
DATABRICKS_TOKEN=your-personal-access-token
```

## 使用方式

### 搭配 Claude Code

在 Claude Code 的 MCP 設定 (`~/.claude.json`) 中加入：

```json
{
  "mcpServers": {
    "databricks-sql": {
      "type": "stdio",
      "command": "uv",
      "args": [
        "--directory",
        "/path/to/databricks-mcp",
        "run",
        "python",
        "server.py"
      ],
      "env": {
        "DATABRICKS_SERVER_HOSTNAME": "your-workspace.cloud.databricks.com",
        "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/your-warehouse-id",
        "DATABRICKS_TOKEN": "your-token"
      }
    }
  }
}
```

### 獨立執行

```bash
uv run python server.py
```

## 可用工具

### SQL 與 Metadata
| 工具 | 說明 |
|------|------|
| `databricks_query` | 執行 SQL 查詢（唯讀） |
| `list_catalogs` | 列出所有 catalogs |
| `list_schemas` | 列出 catalog 下的 schemas |
| `list_tables` | 列出 schema 下的 tables |
| `get_table_schema` | 取得資料表結構 (DESCRIBE EXTENDED) |
| `search_tables` | 依名稱搜尋資料表 |

### Delta Lake
| 工具 | 說明 |
|------|------|
| `get_table_history` | 查看 Delta table 變更歷史 |
| `get_table_detail` | 查看 Delta table 詳情 |
| `get_grants` | 查看物件權限 |
| `list_volumes` | 列出 Unity Catalog volumes |

### Jobs 與 Pipelines
| 工具 | 說明 |
|------|------|
| `list_jobs` | 列出 Databricks Jobs |
| `get_job` | 取得 job 詳情 |
| `list_job_runs` | 列出 job 執行歷史 |
| `get_job_run` | 取得執行詳情 |
| `list_pipelines` | 列出 DLT Pipelines |
| `get_pipeline` | 取得 pipeline 狀態 |

### Compute 與 Monitoring
| 工具 | 說明 |
|------|------|
| `list_query_history` | 列出 SQL 查詢歷史 |
| `list_warehouses` | 列出 SQL Warehouses |
| `list_clusters` | 列出 clusters |
| `get_cluster_metrics` | 取得 cluster CPU/memory metrics |
| `get_cluster_events` | 取得 cluster 事件 |

## 專案結構

```
databricks-mcp/
├── server.py         # 進入點
├── core.py           # 共用連線與 MCP 實例
└── tools/
    ├── query.py      # SQL 查詢與 metadata
    ├── delta.py      # Delta Lake 與權限
    ├── jobs.py       # Jobs 管理
    ├── pipelines.py  # DLT Pipelines
    ├── compute.py    # Clusters 與查詢歷史
    └── metrics.py    # Cluster metrics
```

## 安全性

此伺服器**設計為唯讀**：

- ❌ 禁止 INSERT、UPDATE、DELETE、DROP、TRUNCATE、MERGE、COPY
- ✅ 允許 SELECT、SHOW、DESCRIBE、CREATE VIEW
- 憑證透過環境變數傳入（絕不寫死在程式碼中）

## 授權

MIT

## 貢獻

歡迎提交 Issues 和 Pull Requests！
