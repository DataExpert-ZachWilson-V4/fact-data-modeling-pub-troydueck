CREATE OR REPLACE TABLE tdueck66966.monthly_host_activity_reduced (
  host VARCHAR, --'host': stores the host site from the web_events dataset.
  metric_name VARCHAR, --'metric_name': stores the derived value for the metric evaluated from the full url path in the web_events dataset.
  metric_array ARRAY(INTEGER), --'metric_array': stores the array of web activity for the given url path for each day of the month.
  month_start VARCHAR --'month_start': stores the first day of the month to be used as the fixed date to analyze the array
)
WITH
(
  FORMAT = 'PARQUET',
  partitioning = ARRAY['metric_name', 'month_start']
)
