"""
Core module - Shared connections and utility functions
"""
import os
import re
import logging
from typing import List, Dict, Any
from fastmcp import FastMCP, Context
from fastmcp.exceptions import ToolError
from databricks import sql
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv


# Valid SQL identifier pattern (alphanumeric, underscore, hyphen, max 128 chars)
# Databricks allows hyphens in catalog/schema/table names
IDENTIFIER_PATTERN = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_-]{0,127}$')

# Valid securable types for SHOW GRANTS
VALID_SECURABLE_TYPES = frozenset({
    "CATALOG", "SCHEMA", "TABLE", "VIEW", "VOLUME",
    "FUNCTION", "EXTERNAL LOCATION", "STORAGE CREDENTIAL",
    "CONNECTION", "SHARE", "RECIPIENT", "PROVIDER"
})


def validate_identifier(value: str, field_name: str) -> str:
    """Validate SQL identifier to prevent injection"""
    if not value or not IDENTIFIER_PATTERN.match(value):
        raise ToolError(
            f"Invalid {field_name}: must start with letter/underscore, "
            f"contain only alphanumeric/underscore/hyphen, max 128 chars"
        )
    return value


def quote_identifier(value: str) -> str:
    """Quote identifier with backticks for safe SQL usage"""
    # Escape any backticks in the value (double them)
    escaped = value.replace("`", "``")
    return f"`{escaped}`"


def safe_identifier(value: str, field_name: str) -> str:
    """Validate and quote identifier for SQL - returns quoted identifier"""
    validate_identifier(value, field_name)
    return quote_identifier(value)


def validate_securable_type(value: str) -> str:
    """Validate securable type for SHOW GRANTS"""
    normalized = value.strip().upper()
    if normalized not in VALID_SECURABLE_TYPES:
        raise ToolError(
            f"Invalid securable_type: '{value}'. "
            f"Must be one of: {', '.join(sorted(VALID_SECURABLE_TYPES))}"
        )
    return normalized

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("databricks-mcp")

# Create FastMCP instance (singleton)
mcp = FastMCP("Databricks Server")


def get_sql_connection():
    """Create Databricks SQL connection"""
    server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    access_token = os.getenv("DATABRICKS_TOKEN")

    if not all([server_hostname, http_path, access_token]):
        raise ToolError("Missing required SQL env vars: DATABRICKS_SERVER_HOSTNAME, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN")

    return sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=access_token
    )


def get_workspace_client() -> WorkspaceClient:
    """Create Databricks SDK WorkspaceClient"""
    host = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    token = os.getenv("DATABRICKS_TOKEN")

    if not host or not token:
        raise ToolError("Missing required SDK env vars: DATABRICKS_SERVER_HOSTNAME, DATABRICKS_TOKEN")

    return WorkspaceClient(host=f"https://{host}", token=token)


def execute_sql(ctx: Context, sql_query: str) -> List[Dict[str, Any]]:
    """Execute SQL query helper function"""
    ctx.info(f"Executing query: {sql_query[:100]}...")
    try:
        connection = get_sql_connection()
        cursor = connection.cursor()
        cursor.execute(sql_query)

        if cursor.description:
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            results = [dict(zip(columns, row)) for row in rows]
            ctx.info(f"Returned {len(results)} rows")
        else:
            results = [{"status": "success", "message": "Command executed successfully"}]
            ctx.info("Execution successful")

        cursor.close()
        connection.close()
        return results
    except Exception as e:
        raise ToolError(f"SQL execution failed: {str(e)}")


def utc_to_taipei(utc_str: str) -> str:
    """Convert UTC time string to Taipei time (UTC+8)"""
    from datetime import datetime, timedelta
    if not utc_str:
        return None
    try:
        utc_str = str(utc_str).replace('T', ' ').split('.')[0]
        dt = datetime.strptime(utc_str, "%Y-%m-%d %H:%M:%S")
        taipei_dt = dt + timedelta(hours=8)
        return taipei_dt.strftime("%Y-%m-%d %H:%M:%S")
    except:
        return utc_str
