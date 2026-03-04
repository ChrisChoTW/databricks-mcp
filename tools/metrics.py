"""
Cluster Metrics (System Tables)
"""
import re
from typing import List, Dict, Any
from datetime import datetime, timedelta
from fastmcp import Context
from fastmcp.exceptions import ToolError
from core import mcp, get_workspace_client, execute_sql, utc_to_taipei

# Cluster ID pattern: MMDD-HHMMSS-xxxxxxxx
CLUSTER_ID_PATTERN = re.compile(r'^[0-9]{4}-[0-9]{6}-[a-z0-9]{8}$')
# ISO datetime pattern
DATETIME_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}(T|\s)\d{2}:\d{2}:\d{2}$')


@mcp.tool
def get_cluster_metrics(
    ctx: Context,
    cluster_id: str,
    start_time: str = None,
    end_time: str = None,
    limit: int = 60
) -> Dict[str, Any]:
    """
    Get cluster CPU/Memory/Network/Disk metrics

    Data source: system.compute.node_timeline (one record per minute)

    Args:
        cluster_id: Cluster ID
        start_time: Start time (ISO format), defaults to last 1 hour
        end_time: End time (ISO format), defaults to now
        limit: Max number of records to return, default 60 (1 hour)

    Returns:
        Metrics time series and summary statistics
    """
    # Validate cluster_id format
    if not CLUSTER_ID_PATTERN.match(cluster_id):
        raise ToolError("Invalid cluster_id format")

    time_condition = f"cluster_id = '{cluster_id}'"
    if start_time:
        if not DATETIME_PATTERN.match(start_time):
            raise ToolError("Invalid start_time format. Use ISO format: YYYY-MM-DDTHH:MM:SS")
        time_condition += f" AND start_time >= '{start_time}'"
    if end_time:
        if not DATETIME_PATTERN.match(end_time):
            raise ToolError("Invalid end_time format. Use ISO format: YYYY-MM-DDTHH:MM:SS")
        time_condition += f" AND end_time <= '{end_time}'"

    metrics_sql = f"""
    SELECT
        start_time,
        end_time,
        instance_id,
        driver,
        node_type,
        ROUND(cpu_user_percent, 2) as cpu_user_pct,
        ROUND(cpu_system_percent, 2) as cpu_system_pct,
        ROUND(cpu_wait_percent, 2) as cpu_wait_pct,
        ROUND(cpu_user_percent + cpu_system_percent, 2) as cpu_total_pct,
        ROUND(mem_used_percent, 2) as mem_used_pct,
        ROUND(mem_swap_percent, 2) as mem_swap_pct,
        network_sent_bytes,
        network_received_bytes,
        disk_free_bytes_per_mount_point
    FROM system.compute.node_timeline
    WHERE {time_condition}
    ORDER BY start_time DESC
    LIMIT {limit}
    """

    ctx.info(f"Querying cluster {cluster_id} metrics...")
    metrics = execute_sql(ctx, metrics_sql)

    if not metrics:
        return {
            "cluster_id": cluster_id,
            "error": "No metrics data found, cluster may not be running or out of time range",
            "metrics": [],
            "summary": {}
        }

    cpu_totals = [float(m.get("cpu_total_pct", 0) or 0) for m in metrics]
    mem_used = [float(m.get("mem_used_pct", 0) or 0) for m in metrics]

    for m in metrics:
        m["time_local"] = utc_to_taipei(m.get("start_time"))
        m["start_time"] = str(m.get("start_time"))
        m["end_time"] = str(m.get("end_time"))

    summary = {
        "data_points": len(metrics),
        "time_range_local": {
            "start": utc_to_taipei(metrics[-1].get("start_time")) if metrics else None,
            "end": utc_to_taipei(metrics[0].get("end_time")) if metrics else None
        },
        "cpu": {
            "avg_pct": round(sum(cpu_totals) / len(cpu_totals), 2) if cpu_totals else 0,
            "max_pct": round(max(cpu_totals), 2) if cpu_totals else 0,
            "min_pct": round(min(cpu_totals), 2) if cpu_totals else 0
        },
        "memory": {
            "avg_pct": round(sum(mem_used) / len(mem_used), 2) if mem_used else 0,
            "max_pct": round(max(mem_used), 2) if mem_used else 0,
            "min_pct": round(min(mem_used), 2) if mem_used else 0
        },
        "network": {
            "total_sent_gb": round(sum(int(m.get("network_sent_bytes", 0) or 0) for m in metrics) / 1024**3, 3),
            "total_received_gb": round(sum(int(m.get("network_received_bytes", 0) or 0) for m in metrics) / 1024**3, 3)
        }
    }

    return {
        "cluster_id": cluster_id,
        "metrics": metrics,
        "summary": summary
    }


@mcp.tool
def get_cluster_events(ctx: Context, cluster_id: str, limit: int = 20) -> List[Dict[str, Any]]:
    """
    Get cluster event history (start, terminate, resize, errors, etc.)

    Args:
        cluster_id: Cluster ID
        limit: Max number of records to return

    Returns:
        Event list (time in local timezone)
    """
    w = get_workspace_client()
    ctx.info(f"Querying cluster {cluster_id} events...")

    events_iter = w.clusters.events(cluster_id=cluster_id, limit=limit)

    results = []
    for event in events_iter:
        e = event.as_dict()
        ts = e.get("timestamp")
        if ts:
            local_time = datetime.utcfromtimestamp(ts / 1000) + timedelta(hours=8)
            time_str = local_time.strftime("%Y-%m-%d %H:%M:%S")
        else:
            time_str = None

        results.append({
            "time_local": time_str,
            "timestamp_ms": ts,
            "type": e.get("type"),
            "details": e.get("details")
        })

    return results


@mcp.tool
def get_run_task_metrics(ctx: Context, run_id: int) -> Dict[str, Any]:
    """
    Get job run task execution time details

    Args:
        run_id: Job Run ID

    Returns:
        Task setup/execute/cleanup times (time in local timezone)
    """
    w = get_workspace_client()
    ctx.info(f"Querying run {run_id} task metrics...")

    run = w.jobs.get_run(run_id=run_id)
    run_dict = run.as_dict()

    def ms_to_local(ms):
        if not ms:
            return None
        local_time = datetime.utcfromtimestamp(ms / 1000) + timedelta(hours=8)
        return local_time.strftime("%Y-%m-%d %H:%M:%S")

    tasks = []
    total_duration = 0

    if run_dict.get("tasks"):
        for t in run_dict["tasks"]:
            setup_ms = t.get("setup_duration", 0) or 0
            exec_ms = t.get("execution_duration", 0) or 0
            cleanup_ms = t.get("cleanup_duration", 0) or 0
            total_ms = setup_ms + exec_ms + cleanup_ms
            total_duration += total_ms

            tasks.append({
                "task_key": t.get("task_key"),
                "state": t.get("state", {}).get("result_state") or t.get("state", {}).get("life_cycle_state"),
                "setup_sec": round(setup_ms / 1000, 1),
                "execution_sec": round(exec_ms / 1000, 1),
                "cleanup_sec": round(cleanup_ms / 1000, 1),
                "total_sec": round(total_ms / 1000, 1),
                "cluster_id": t.get("existing_cluster_id") or t.get("cluster_instance", {}).get("cluster_id")
            })

    tasks.sort(key=lambda x: x["execution_sec"], reverse=True)

    start_ms = run_dict.get("start_time")
    end_ms = run_dict.get("end_time")
    duration_min = round((end_ms - start_ms) / 1000 / 60, 2) if start_ms and end_ms else None

    return {
        "run_id": run_id,
        "job_id": run_dict.get("job_id"),
        "state": run_dict.get("state", {}).get("result_state"),
        "start_time_local": ms_to_local(start_ms),
        "end_time_local": ms_to_local(end_ms),
        "duration_min": duration_min,
        "total_task_duration_sec": round(total_duration / 1000, 1),
        "tasks": tasks,
        "slowest_task": tasks[0]["task_key"] if tasks else None
    }
