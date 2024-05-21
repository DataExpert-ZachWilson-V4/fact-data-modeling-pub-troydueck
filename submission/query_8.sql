INSERT INTO tdueck66966.monthly_host_activity_reduced
WITH yesterday AS ( --CTE for yesterday's data for user web activity by host already populated in the monthly_host_activity_reduced table.
  SELECT
    *
  FROM tdueck66966.monthly_host_activity_reduced
  WHERE month_start = '2023-08-01' --hard-coded date for month start that could be parameterized in other tools as Trino appears to be limited.
),
today AS ( --CTE for today's data from the web events dataset to capture the daily metric_value to load into the monthly array.
  SELECT
    host,
    'visited_home_page' AS metric_name, --hard-coded value derived from the url; also ran INSERT INTO with 'visited_signup' as metric_name
    COUNT( --counts the number of visits by host and metric name (follows the GROUP BY)
      CASE WHEN url = '/' THEN 1 --evaluates the url to derive a metric; also ran INSERT INTO with '/signup' as metric_value
      END ) as metric_value,
    CAST(event_time AS DATE) as DATE
  FROM bootcamp.web_events
  WHERE CAST(event_time AS DATE) = DATE('2023-08-03') --hard-coded date for daily activity that could be parameterized in other tools as Trino appears to be limited.
  GROUP BY host, CAST(event_time AS DATE)
)

SELECT
  COALESCE(t.host, y.host) as host, --coalesced columns to capture both yesterday's and today's values when there isn't a change.
  COALESCE(t.metric_name, y.metric_name) as metric_name, --coalesced columns to capture both yesterday's and today's values when there isn't a change.
  COALESCE(y.metric_array, --coalesced columns to capture both yesterday's and today's values when there isn't a change.
    REPEAT(NULL, --ensures nulls are populated for the first dates where data doesn't appear until later in the array. 
      CAST(DATE_DIFF('day', DATE('2023-08-01'), t.date) AS INTEGER) --provides the delta from the month start to the first day data is populated in the array to properly backfill the array.
      )
    ) || ARRAY[t.metric_value] AS metric_array, --concatenates yesterday's coalesced array with today's array
  '2023-08-01' AS month_start --hard-coded date for month start that could be parameterized in other tools as Trino appears to be limited
FROM today t
FULL OUTER JOIN yesterday y ON t.host = y.host
  AND t.metric_name = y.metric_name
