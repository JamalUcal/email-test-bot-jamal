SELECT
  table_id,
  ROUND(size_bytes / (1024 * 1024 * 1024), 2) AS size_gb,
  row_count,
  TIMESTAMP_MILLIS(creation_time) AS created,
  TIMESTAMP_MILLIS(last_modified_time) AS last_modified
FROM `PRICING.__TABLES__`
ORDER BY size_bytes DESC;