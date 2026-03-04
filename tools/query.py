"""
SQL Queries and Metadata (Catalogs, Schemas, Tables)
"""
from typing import List, Dict, Any, Optional
from fastmcp import Context
from fastmcp.exceptions import ToolError
from core import mcp, execute_sql, safe_identifier


@mcp.tool
def databricks_query(ctx: Context, sql_query: Optional[str] = None, sql: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Execute Databricks SQL query (supports SELECT, SHOW, DESCRIBE, CREATE, ALTER).
    INSERT, UPDATE, DELETE, DROP and other destructive operations are blocked.

    Args:
        sql_query: SQL query statement (preferred parameter)
        sql: SQL query statement (fallback for backward compatibility)

    Returns:
        Query results as list of dicts
    """
    query_to_execute = sql_query if sql_query is not None else sql

    if query_to_execute is None:
        raise ToolError("Must provide sql_query or sql parameter.")

    query_upper = query_to_execute.strip().upper()
    forbidden_keywords = [
        "INSERT INTO", "UPDATE ", "DELETE FROM", "DROP TABLE", "DROP VIEW",
        "DROP SCHEMA", "DROP CATALOG", "TRUNCATE TABLE", "MERGE INTO", "COPY INTO"
    ]
    for keyword in forbidden_keywords:
        if keyword in query_upper:
            raise ToolError(f"Destructive operation not allowed: {keyword}")
    return execute_sql(ctx, query_to_execute)


@mcp.tool
def list_catalogs(ctx: Context) -> List[Dict[str, Any]]:
    """List all catalogs"""
    return execute_sql(ctx, "SHOW CATALOGS")


@mcp.tool
def list_schemas(ctx: Context, catalog_name: str) -> List[Dict[str, Any]]:
    """List schemas in the specified catalog"""
    catalog = safe_identifier(catalog_name, "catalog_name")
    return execute_sql(ctx, f"SHOW SCHEMAS IN {catalog}")


@mcp.tool
def list_tables(ctx: Context, catalog_name: str, schema_name: str) -> List[Dict[str, Any]]:
    """List tables in the specified schema"""
    catalog = safe_identifier(catalog_name, "catalog_name")
    schema = safe_identifier(schema_name, "schema_name")
    return execute_sql(ctx, f"SHOW TABLES IN {catalog}.{schema}")


@mcp.tool
def get_table_schema(ctx: Context, catalog_name: str, schema_name: str, table_name: str) -> List[Dict[str, Any]]:
    """Get table structure (DESCRIBE EXTENDED)"""
    catalog = safe_identifier(catalog_name, "catalog_name")
    schema = safe_identifier(schema_name, "schema_name")
    table = safe_identifier(table_name, "table_name")
    return execute_sql(ctx, f"DESCRIBE EXTENDED {catalog}.{schema}.{table}")


@mcp.tool
def search_tables(ctx: Context, keyword: str, catalog: str = None) -> List[Dict[str, Any]]:
    """Search tables by name (using information_schema)"""
    if not catalog:
        raise ToolError("Must specify catalog parameter")

    cat = safe_identifier(catalog, "catalog")
    safe_identifier(keyword, "keyword")  # validate only, no quote needed for LIKE

    sql = f"""
    SELECT table_catalog, table_schema, table_name, table_type, comment
    FROM {cat}.information_schema.tables
    WHERE table_name LIKE '%{keyword}%'
    LIMIT 50
    """
    return execute_sql(ctx, sql)
