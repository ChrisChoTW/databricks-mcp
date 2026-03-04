"""
Pipelines (DLT) Management (SDK)
"""
from typing import List, Dict, Any
from fastmcp import Context
from core import mcp, get_workspace_client


@mcp.tool
def list_pipelines(ctx: Context, limit: int = 20) -> List[Dict[str, Any]]:
    """List Delta Live Tables Pipelines"""
    w = get_workspace_client()
    pipes = w.pipelines.list_pipelines(max_results=limit)
    return [p.as_dict() for p in pipes]


@mcp.tool
def get_pipeline(ctx: Context, pipeline_id: str) -> Dict[str, Any]:
    """Get pipeline status"""
    w = get_workspace_client()
    return w.pipelines.get(pipeline_id=pipeline_id).as_dict()


@mcp.tool
def list_pipeline_updates(ctx: Context, pipeline_id: str, limit: int = 10) -> List[Dict[str, Any]]:
    """List pipeline update history"""
    w = get_workspace_client()
    updates = w.pipelines.list_pipeline_events(pipeline_id=pipeline_id, max_results=limit)
    return [u.as_dict() for u in updates]
