# Contributing to databricks-mcp

Thank you for your interest in contributing! This document provides guidelines for contributing to the project.

## Getting Started

### Prerequisites

- Python 3.13+
- [uv](https://github.com/astral-sh/uv) package manager

### Development Setup

```bash
# Clone the repository
git clone https://github.com/ChrisChoTW/databricks-mcp.git
cd databricks-mcp

# Install dependencies
uv sync

# Copy environment template
cp .env.example .env
# Edit .env with your Databricks credentials
```

## How to Contribute

### Reporting Issues

- Use GitHub Issues to report bugs or request features
- Include clear reproduction steps for bugs
- Provide environment details (Python version, OS, etc.)

### Pull Requests

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/your-feature`)
3. Make your changes
4. Test your changes locally
5. Commit with clear messages
6. Push to your fork
7. Open a Pull Request

### Commit Message Format

We use [Gitmoji](https://gitmoji.dev/) for commit messages:

- `🎉 feat:` New feature
- `🐛 fix:` Bug fix
- `📝 docs:` Documentation changes
- `♻️ refactor:` Code refactoring
- `🧪 test:` Adding tests

### Code Style

- Follow PEP 8 guidelines
- Use type hints for function parameters and return values
- Add docstrings for public functions

## Project Structure

```
databricks-mcp/
├── server.py          # Entry point
├── core.py            # Core utilities (connections, SQL execution)
├── tools/             # MCP tool modules
│   ├── query.py       # SQL queries and metadata
│   ├── delta.py       # Delta Lake operations
│   ├── jobs.py        # Jobs management
│   ├── pipelines.py   # DLT pipelines
│   ├── compute.py     # Clusters and warehouses
│   ├── metrics.py     # Cluster metrics
│   └── dashboards.py  # Dashboard management
└── docs/              # Documentation
```

## Adding New Tools

1. Create or modify a file in `tools/`
2. Import `mcp` from `core.py`
3. Use `@mcp.tool` decorator for your function
4. Add proper docstring and type hints
5. Update README.md with the new tool

Example:

```python
from core import mcp, execute_sql

@mcp.tool
def my_new_tool(ctx: Context, param: str) -> List[Dict[str, Any]]:
    """Tool description here"""
    return execute_sql(ctx, f"SELECT * FROM {param}")
```

## Questions?

Feel free to open an issue for any questions or discussions.
