# databricks-mcp

[![MCP](https://img.shields.io/badge/MCP-Server-blue)](https://modelcontextprotocol.io)
[![Python](https://img.shields.io/badge/Python-3.13+-green)](https://python.org)
[![License](https://img.shields.io/badge/License-MIT-yellow)](LICENSE)

**Read this in other languages: [正體中文](docs/README-zh-tw.md)**

A **read-only** MCP (Model Context Protocol) server for Databricks, enabling Claude to query Databricks SQL, browse metadata, and monitor jobs/pipelines.

## Features

- **SQL Queries**: Execute SELECT, SHOW, DESCRIBE queries (write operations blocked)
- **Metadata Browsing**: List catalogs, schemas, tables, and search tables
- **Delta Lake**: View table history, details, and grants
- **Jobs & Pipelines**: List and monitor Databricks Jobs and DLT Pipelines
- **Query History**: Browse SQL query history with filters
- **Cluster Metrics**: Monitor CPU, memory, network usage from system tables

## Installation

### Prerequisites

- Python 3.13+
- [uv](https://github.com/astral-sh/uv) package manager
- Databricks workspace with SQL Warehouse

### Setup

```bash
# Clone the repository
git clone https://github.com/ChrisChoTW/databricks-mcp.git
cd databricks-mcp

# Install dependencies
uv sync

# Create .env file
cp .env.example .env
```

### Configuration

Edit `.env` with your Databricks credentials:

```env
DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
DATABRICKS_TOKEN=your-personal-access-token
```

## Usage

### With Claude Code

Add to your Claude Code MCP configuration (`~/.claude.json`):

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

### Standalone

```bash
uv run python server.py
```

## Available Tools

### SQL & Metadata
| Tool | Description |
|------|-------------|
| `databricks_query` | Execute SQL queries (read-only) |
| `list_catalogs` | List all catalogs |
| `list_schemas` | List schemas in a catalog |
| `list_tables` | List tables in a schema |
| `get_table_schema` | Get table structure (DESCRIBE EXTENDED) |
| `search_tables` | Search tables by name |

### Delta Lake
| Tool | Description |
|------|-------------|
| `get_table_history` | View Delta table change history |
| `get_table_detail` | View Delta table details |
| `get_grants` | View object permissions |
| `list_volumes` | List Unity Catalog volumes |

### Jobs & Pipelines
| Tool | Description |
|------|-------------|
| `list_jobs` | List Databricks Jobs |
| `get_job` | Get job details |
| `list_job_runs` | List job run history |
| `get_job_run` | Get run details |
| `list_pipelines` | List DLT Pipelines |
| `get_pipeline` | Get pipeline status |

### Compute & Monitoring
| Tool | Description |
|------|-------------|
| `list_query_history` | List SQL query history |
| `list_warehouses` | List SQL Warehouses |
| `list_clusters` | List clusters |
| `get_cluster_metrics` | Get cluster CPU/memory metrics |
| `get_cluster_events` | Get cluster events |

## Project Structure

```
databricks-mcp/
├── server.py         # Entry point
├── core.py           # Shared connections and MCP instance
└── tools/
    ├── query.py      # SQL queries and metadata
    ├── delta.py      # Delta Lake and permissions
    ├── jobs.py       # Jobs management
    ├── pipelines.py  # DLT Pipelines
    ├── compute.py    # Clusters and query history
    └── metrics.py    # Cluster metrics
```

## Security

This server is **read-only by design**:

- ❌ INSERT, UPDATE, DELETE, DROP, TRUNCATE, MERGE, COPY blocked
- ✅ SELECT, SHOW, DESCRIBE, CREATE VIEW allowed
- Credentials are passed via environment variables (never hardcoded)

## License

MIT

## Contributing

Issues and pull requests are welcome!
