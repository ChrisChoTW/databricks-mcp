"""
Jobs Management (SDK)
"""
from typing import List, Dict, Any
from fastmcp import Context
from core import mcp, get_workspace_client


@mcp.tool
def list_jobs(ctx: Context, limit: int = 20, expand_tasks: bool = False) -> List[Dict[str, Any]]:
    """List Jobs"""
    w = get_workspace_client()
    jobs_iter = w.jobs.list(expand_tasks=expand_tasks)
    results = []
    for i, job in enumerate(jobs_iter):
        if i >= limit:
            break
        results.append(job.as_dict())
    return results


@mcp.tool
def get_job(ctx: Context, job_id: int) -> Dict[str, Any]:
    """Get job details"""
    w = get_workspace_client()
    return w.jobs.get(job_id=job_id).as_dict()


@mcp.tool
def list_job_runs(ctx: Context, job_id: int, limit: int = 10) -> List[Dict[str, Any]]:
    """List job run history"""
    w = get_workspace_client()
    runs_iter = w.jobs.list_runs(job_id=job_id, expand_tasks=False)
    results = []
    for i, run in enumerate(runs_iter):
        if i >= limit:
            break
        run_d = run.as_dict()
        results.append({
            "run_id": run_d.get("run_id"),
            "job_id": run_d.get("job_id"),
            "state": run_d.get("state"),
            "start_time": run_d.get("start_time"),
            "end_time": run_d.get("end_time"),
            "run_page_url": run_d.get("run_page_url")
        })
    return results


@mcp.tool
def get_job_run(ctx: Context, run_id: int) -> Dict[str, Any]:
    """Get run details"""
    w = get_workspace_client()
    return w.jobs.get_run(run_id=run_id).as_dict()
