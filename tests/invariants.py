def assert_invariants(db):
    checks = {
        "capability_compatibility": """SELECT a.id FROM task_attempts a
            JOIN workers w ON w.id=a.worker_id JOIN tasks t ON t.id=a.task_id
            JOIN jobs j ON j.id=t.job_id
            WHERE NOT (j.operation=ANY(w.supported_operations)) OR t.payload->>'operation'<>j.operation""",
        "capacity": """SELECT w.id FROM workers w JOIN task_attempts a ON a.worker_id=w.id
            AND a.status IN ('ASSIGNED','RUNNING') GROUP BY w.id HAVING count(*)>w.capacity""",
        "active_pair": """SELECT t.id FROM tasks t LEFT JOIN task_attempts a ON a.task_id=t.id
            AND a.status IN ('ASSIGNED','RUNNING') WHERE
            (t.status IN ('ASSIGNED','RUNNING') AND (a.id IS NULL OR t.status<>a.status))
            OR (t.status NOT IN ('ASSIGNED','RUNNING') AND a.id IS NOT NULL)""",
        "attempt_budget": """SELECT t.id FROM tasks t LEFT JOIN task_attempts a ON a.task_id=t.id
            GROUP BY t.id HAVING t.attempt_count<>count(a.id) OR t.attempt_count<>coalesce(max(a.attempt_number),0)""",
        "canonical_success": """SELECT t.id FROM tasks t LEFT JOIN task_attempts a ON a.task_id=t.id AND a.status='SUCCEEDED'
            WHERE (t.status='COMPLETED')<>(a.id IS NOT NULL)""",
        "job_counters": """SELECT j.id FROM jobs j JOIN tasks t ON t.job_id=j.id GROUP BY j.id
            HAVING count(*)<>j.task_count OR count(*) FILTER (WHERE t.status='COMPLETED')<>j.completed_tasks
            OR count(*) FILTER (WHERE t.status='FAILED')<>j.failed_tasks""",
    }
    for name, query in checks.items():
        assert db.run(lambda c: c.execute(query).fetchall()) == [], f"Persistent invariant violated: {name}"
