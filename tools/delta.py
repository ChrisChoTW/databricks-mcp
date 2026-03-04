"""
Delta Lake and Unity Catalog Permissions
"""
import json
from typing import List, Dict, Any, Set
from fastmcp import Context
from core import mcp, execute_sql, get_workspace_client, safe_identifier, validate_identifier, validate_securable_type, quote_identifier


@mcp.tool
def get_table_history(ctx: Context, catalog: str, schema: str, table: str, limit: int = 10) -> List[Dict[str, Any]]:
    """View Delta table change history (DESCRIBE HISTORY)"""
    cat = safe_identifier(catalog, "catalog")
    sch = safe_identifier(schema, "schema")
    tbl = safe_identifier(table, "table")
    return execute_sql(ctx, f"DESCRIBE HISTORY {cat}.{sch}.{tbl} LIMIT {limit}")


@mcp.tool
def get_table_detail(ctx: Context, catalog: str, schema: str, table: str) -> List[Dict[str, Any]]:
    """View Delta table details (DESCRIBE DETAIL)"""
    cat = safe_identifier(catalog, "catalog")
    sch = safe_identifier(schema, "schema")
    tbl = safe_identifier(table, "table")
    return execute_sql(ctx, f"DESCRIBE DETAIL {cat}.{sch}.{tbl}")


@mcp.tool
def get_grants(ctx: Context, securable_type: str, full_name: str) -> List[Dict[str, Any]]:
    """
    View object permissions (SHOW GRANTS)

    Args:
        securable_type: Object type (TABLE, SCHEMA, CATALOG, VOLUME, etc.)
        full_name: Full object name (catalog.schema.table format)
    """
    validated_type = validate_securable_type(securable_type)
    # Validate and quote full_name parts
    parts = full_name.split(".")
    quoted_parts = []
    for part in parts:
        validate_identifier(part, "full_name component")
        quoted_parts.append(quote_identifier(part))
    quoted_full_name = ".".join(quoted_parts)
    return execute_sql(ctx, f"SHOW GRANTS ON {validated_type} {quoted_full_name}")


@mcp.tool
def list_volumes(ctx: Context, catalog: str, schema: str) -> List[Dict[str, Any]]:
    """List Unity Catalog Volumes"""
    cat = safe_identifier(catalog, "catalog")
    sch = safe_identifier(schema, "schema")
    return execute_sql(ctx, f"SHOW VOLUMES IN {cat}.{sch}")


def _resolve_job_info(w, job_id: str, notebook_id: str) -> Dict[str, Any]:
    """Resolve job and notebook details from IDs"""
    result = {
        "job_id": job_id,
        "job_name": None,
        "notebook_id": notebook_id,
        "notebook_path": None,
        "task_key": None
    }

    try:
        job = w.jobs.get(job_id=job_id)
        result["job_name"] = job.settings.name if job.settings else None

        # Find notebook path from job tasks
        if job.settings and job.settings.tasks:
            for task in job.settings.tasks:
                if hasattr(task, 'notebook_task') and task.notebook_task:
                    nb_path = task.notebook_task.notebook_path
                    try:
                        nb_status = w.workspace.get_status(nb_path)
                        if str(nb_status.object_id) == notebook_id:
                            result["notebook_path"] = nb_path
                            result["task_key"] = task.task_key
                            break
                    except Exception:
                        pass
    except Exception:
        pass

    return result


@mcp.tool
def get_table_lineage(
    ctx: Context,
    catalog: str,
    schema: str,
    table: str,
    include_notebooks: bool = False,
    limit: int = 50
) -> Dict[str, Any]:
    """
    Get table lineage (upstream/downstream tables and related notebooks/jobs)

    Args:
        catalog: Catalog name
        schema: Schema name
        table: Table name
        include_notebooks: Include notebook/job associations (slower)
        limit: Max rows to return (default 50)

    Returns:
        Dict with upstream, downstream tables and optionally notebook/job info
    """
    cat = safe_identifier(catalog, "catalog")
    sch = safe_identifier(schema, "schema")
    tbl = safe_identifier(table, "table")
    full_name = f"{cat}.{sch}.{tbl}"

    # Query system.access.table_lineage
    sql_query = f"""
    SELECT DISTINCT
        source_table_full_name,
        target_table_full_name,
        entity_type,
        entity_metadata
    FROM system.access.table_lineage
    WHERE source_table_full_name = '{full_name}'
       OR target_table_full_name = '{full_name}'
    ORDER BY source_table_full_name, target_table_full_name
    LIMIT {limit}
    """

    rows = execute_sql(ctx, sql_query)

    # Classify upstream/downstream
    upstream: Set[str] = set()
    downstream: Set[str] = set()
    notebook_reads: Dict[str, Dict] = {}
    notebook_writes: Dict[str, Dict] = {}

    # Collect job/notebook pairs for resolution
    job_notebook_pairs: List[Dict] = []

    for row in rows:
        source = row.get("source_table_full_name")
        target = row.get("target_table_full_name")
        metadata_str = row.get("entity_metadata")

        # Classify tables
        if source == full_name and target and target != full_name:
            downstream.add(target)
        elif target == full_name and source and source != full_name:
            upstream.add(source)

        # Parse notebook/job info if requested
        if include_notebooks and metadata_str:
            try:
                metadata = json.loads(metadata_str) if isinstance(metadata_str, str) else metadata_str
                notebook_id = metadata.get("notebook_id")
                job_info = metadata.get("job_info", {})
                job_id = job_info.get("job_id") if job_info else None

                if notebook_id and job_id:
                    job_notebook_pairs.append({
                        "notebook_id": notebook_id,
                        "job_id": job_id,
                        "source": source,
                        "target": target
                    })
            except (json.JSONDecodeError, TypeError):
                pass

    result = {
        "table": full_name,
        "upstream": sorted(upstream),
        "downstream": sorted(downstream),
        "upstream_count": len(upstream),
        "downstream_count": len(downstream)
    }

    # Resolve notebook/job details if requested
    if include_notebooks and job_notebook_pairs:
        ctx.info(f"Resolving {len(job_notebook_pairs)} notebook/job associations...")
        w = get_workspace_client()

        for pair in job_notebook_pairs:
            key = f"{pair['job_id']}:{pair['notebook_id']}"
            info = _resolve_job_info(w, pair["job_id"], pair["notebook_id"])

            if pair["source"] == full_name:
                notebook_reads[key] = info
            elif pair["target"] == full_name:
                notebook_writes[key] = info

        result["notebooks_reading"] = list(notebook_reads.values())
        result["notebooks_writing"] = list(notebook_writes.values())

    return result
