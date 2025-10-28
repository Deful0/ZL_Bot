SELECT
    table_name,
    CASE
        WHEN execute_completed_at::date = CURRENT_DATE
        THEN 1
        ELSE 0
    END AS flag_updates
FROM dev.monitoring_mart;