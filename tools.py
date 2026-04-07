import json
import os   
from dotenv import load_dotenv


# ── Agent Tools ───────────────────────────────────────────────────────────────

@tool
def get_all_production_lines() -> str:
    """
    Return all production lines with their plant, department, and line name.
    Use this to understand the structure before doing any analysis.
    """
    rows = run_query("""
        SELECT line_id, plant_name, department_name, line_name
        FROM ProductionLines
        ORDER BY plant_name, department_name, line_name
    """)
    return json.dumps(rows, indent=2, default=str)


@tool
def get_total_downtime_per_line(start_date: str = "", end_date: str = "") -> str:
    """
    Return total downtime minutes per production line, sorted highest first.
    Optionally filter by start_date and end_date (YYYY-MM-DD format).
    NULL downtime_reason rows (0-minute entries) are excluded from totals.
    """
    filters = ["d.downtime_minutes > 0"]
    params: list = []

    if start_date:
        filters.append("d.downtime_date >= ?")
        params.append(start_date)
    if end_date:
        filters.append("d.downtime_date <= ?")
        params.append(end_date)

    where = "WHERE " + " AND ".join(filters)
    sql = f"""
        SELECT
            p.line_name,
            p.department_name,
            p.plant_name,
            ISNULL(SUM(d.downtime_minutes), 0)        AS total_minutes,
            ISNULL(COUNT(d.id), 0)                    AS incident_count,
            ISNULL(AVG(CAST(d.downtime_minutes AS FLOAT)), 0) AS avg_minutes,
            ISNULL(MAX(d.downtime_minutes), 0)         AS max_incident
        FROM ProductionLines p
        LEFT JOIN DownTime d ON p.line_id = d.line_id {where.replace('WHERE','AND') if start_date or end_date else ''}
        {'WHERE d.downtime_minutes > 0' if not (start_date or end_date) else where}
        GROUP BY p.line_id, p.line_name, p.department_name, p.plant_name
        ORDER BY total_minutes DESC
    """
    # Cleaner rewrite to avoid the conditional above:
    filter_clause = " AND ".join(filters)
    sql = f"""
        SELECT
            p.line_name,
            p.department_name,
            p.plant_name,
            ISNULL(SUM(d.downtime_minutes), 0)                  AS total_minutes,
            COUNT(d.id)                                          AS incident_count,
            ISNULL(AVG(CAST(d.downtime_minutes AS FLOAT)), 0)   AS avg_minutes,
            ISNULL(MAX(d.downtime_minutes), 0)                   AS max_incident
        FROM ProductionLines p
        LEFT JOIN DownTime d ON p.line_id = d.line_id
            AND {filter_clause}
        GROUP BY p.line_id, p.line_name, p.department_name, p.plant_name
        ORDER BY total_minutes DESC
    """
    rows = run_query(sql, tuple(params))
    return json.dumps(rows, indent=2, default=str)



@tool
def get_daily_downtime(line_name: str = "", days: int = 7) -> str:
    """
    Return daily downtime totals for the last N days.
    Optionally filter by line_name (e.g. 'Alpha1').
    Use this when the user asks for trends or recent data.
    """
    cutoff = (datetime.today() - timedelta(days=days)).strftime("%Y-%m-%d")
    extra = "AND p.line_name = ?" if line_name else ""
    params: list = [cutoff]
    if line_name:
        params.append(line_name)

    sql = f"""
        SELECT
            p.line_name,
            CONVERT(VARCHAR(10), d.downtime_date, 120) AS downtime_date,
            SUM(d.downtime_minutes) AS daily_minutes,
            COUNT(d.id)             AS incident_count
        FROM ProductionLines p
        JOIN DownTime d ON p.line_id = d.line_id
        WHERE d.downtime_date >= ?
          AND d.downtime_minutes > 0
          {extra}
        GROUP BY p.line_name, d.downtime_date
        ORDER BY d.downtime_date, p.line_name
    """
    rows = run_query(sql, tuple(params))
    return json.dumps(rows, indent=2, default=str)


@tool
def compare_lines(line_a: str, line_b: str, days: int = 30) -> str:
    """
    Compare two production lines head-to-head over the last N days.
    line_a and line_b should be line_name values like 'Alpha1', 'Beta2'.
    """
    cutoff = (datetime.today() - timedelta(days=days)).strftime("%Y-%m-%d")
    sql = """
        SELECT
            p.line_name,
            p.department_name,
            ISNULL(SUM(d.downtime_minutes), 0)                AS total_minutes,
            COUNT(d.id)                                        AS incident_count,
            ISNULL(AVG(CAST(d.downtime_minutes AS FLOAT)), 0) AS avg_minutes,
            ISNULL(MAX(d.downtime_minutes), 0)                 AS max_single
        FROM ProductionLines p
        LEFT JOIN DownTime d ON p.line_id = d.line_id
            AND d.downtime_date >= ?
            AND d.downtime_minutes > 0
        WHERE p.line_name IN (?, ?)
        GROUP BY p.line_id, p.line_name, p.department_name
    """
    rows = run_query(sql, (cutoff, line_a, line_b))
    return json.dumps(rows, indent=2, default=str)


@tool
def get_downtime_by_reason(line_name: str = "", days: int = 30) -> str:
    """
    Break down downtime by downtime_reason. NULL reasons are labelled 'No downtime'.
    Optionally filter by a specific line_name.
    """
    cutoff = (datetime.today() - timedelta(days=days)).strftime("%Y-%m-%d")
    extra = "AND p.line_name = ?" if line_name else ""
    params: list = [cutoff]
    if line_name:
        params.append(line_name)

    sql = f"""
        SELECT
            ISNULL(d.downtime_reason, 'No downtime') AS reason,
            COUNT(d.id)                               AS occurrences,
            SUM(d.downtime_minutes)                   AS total_minutes
        FROM ProductionLines p
        JOIN DownTime d ON p.line_id = d.line_id
        WHERE d.downtime_date >= ?
          AND d.downtime_minutes > 0
          {extra}
        GROUP BY d.downtime_reason
        ORDER BY total_minutes DESC
    """
    rows = run_query(sql, tuple(params))
    return json.dumps(rows, indent=2, default=str)


@tool
def get_department_summary(days: int = 30) -> str:
    """
    Summarise downtime grouped by department (Alpha dept vs Beta dept).
    Good for high-level department comparison questions.
    """
    cutoff = (datetime.today() - timedelta(days=days)).strftime("%Y-%m-%d")
    sql = """
        SELECT
            p.department_name,
            COUNT(DISTINCT p.line_id)                          AS line_count,
            ISNULL(SUM(d.downtime_minutes), 0)                AS total_minutes,
            COUNT(d.id)                                        AS incident_count,
            ISNULL(AVG(CAST(d.downtime_minutes AS FLOAT)), 0) AS avg_per_incident
        FROM ProductionLines p
        LEFT JOIN DownTime d ON p.line_id = d.line_id
            AND d.downtime_date >= ?
            AND d.downtime_minutes > 0
        GROUP BY p.department_name
        ORDER BY total_minutes DESC
    """
    rows = run_query(sql, (cutoff,))
    return json.dumps(rows, indent=2, default=str)
