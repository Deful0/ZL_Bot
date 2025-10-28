UPDATE dev.monitoring_mart
SET execute_completed_at = latest_results.execute_completed_at
FROM (
        SELECT DISTINCT ON (name)
             name,
             execute_completed_at
        FROM elementary.dbt_run_results
        ORDER BY name, execute_completed_at DESC
     ) AS latest_results
WHERE monitoring_mart.table_name = latest_results.name;