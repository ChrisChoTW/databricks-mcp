#!/usr/bin/env python3
"""
Databricks MCP Server

Provides Databricks SQL queries, Jobs, Pipelines, Query History, and Metadata browsing.
"""
from core import mcp

# Import tools module to register all MCP tools
import tools  # noqa: F401


def main():
    mcp.run()


if __name__ == "__main__":
    main()
