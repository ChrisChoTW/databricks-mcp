"""
Query History, Warehouses, Clusters, Workspace
"""
from typing import List, Dict, Any
from datetime import datetime
from fastmcp import Context
from core import mcp, get_workspace_client


@mcp.tool
def list_query_history(
    ctx: Context,
    warehouse_id: str = None,
    user_id: str = None,
    start_time: str = None,
    end_time: str = None,
    limit: int = 20
) -> List[Dict[str, Any]]:
    """
    List SQL query history

    Args:
        warehouse_id: (Optional) Filter by specific warehouse
        user_id: (Optional) Filter by specific user
        start_time: (Optional) Start time in local format "YYYY-MM-DD HH:MM:SS"
        end_time: (Optional) End time in local format "YYYY-MM-DD HH:MM:SS"
        limit: Number of results to return
    """
    from databricks.sdk.service.sql import QueryFilter, TimeRange

    w = get_workspace_client()

    # Build filter
    filter_kwargs = {}

    if warehouse_id:
        filter_kwargs["warehouse_ids"] = [warehouse_id]

    if user_id:
        filter_kwargs["user_ids"] = [int(user_id)]

    # Time range (local time to Unix timestamp ms)
    if start_time or end_time:
        time_range_kwargs = {}
        if start_time:
            dt = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
            time_range_kwargs["start_time_ms"] = int(dt.timestamp() * 1000)
        if end_time:
            dt = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
            time_range_kwargs["end_time_ms"] = int(dt.timestamp() * 1000)
        filter_kwargs["query_start_time_range"] = TimeRange(**time_range_kwargs)

    if filter_kwargs:
        filter_by = QueryFilter(**filter_kwargs)
        response = w.query_history.list(filter_by=filter_by, max_results=limit)
    else:
        response = w.query_history.list(max_results=limit)

    queries_list = response.res if response and response.res else []

    results = []
    for q in queries_list:
        q_dict = q.as_dict()
        start_ms = q_dict.get("query_start_time_ms")
        start_time_local = None
        if start_ms:
            dt_local = datetime.fromtimestamp(start_ms / 1000)
            start_time_local = dt_local.strftime("%Y-%m-%d %H:%M:%S")

        results.append({
            "query_id": q_dict.get("query_id"),
            "query_text": q_dict.get("query_text"),
            "status": q_dict.get("status"),
            "statement_type": q_dict.get("statement_type"),
            "user_name": q_dict.get("user_name"),
            "duration": q_dict.get("duration"),
            "start_time_ms": start_ms,
            "start_time_local": start_time_local
        })
    return results


@mcp.tool
def list_warehouses(ctx: Context) -> List[Dict[str, Any]]:
    """List SQL Warehouses"""
    w = get_workspace_client()
    return [wh.as_dict() for wh in w.warehouses.list()]


@mcp.tool
def list_clusters(ctx: Context, limit: int = 20) -> List[Dict[str, Any]]:
    """List Clusters"""
    w = get_workspace_client()
    clusters = w.clusters.list()
    results = []
    for i, c in enumerate(clusters):
        if i >= limit:
            break
        c_d = c.as_dict()
        results.append({
            "cluster_id": c_d.get("cluster_id"),
            "cluster_name": c_d.get("cluster_name"),
            "state": c_d.get("state"),
            "driver_node_type_id": c_d.get("driver_node_type_id"),
            "spark_version": c_d.get("spark_version")
        })
    return results


@mcp.tool
def list_workspace(ctx: Context, path: str = "/") -> List[Dict[str, Any]]:
    """List Workspace directory contents"""
    w = get_workspace_client()
    return [obj.as_dict() for obj in w.workspace.list(path)]
