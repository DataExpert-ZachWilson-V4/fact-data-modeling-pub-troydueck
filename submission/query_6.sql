INSERT INTO tdueck66966.hosts_cumulated
WITH yesterday AS ( --CTE for yesterday's data for user web activity by host already populated in the hosts_cumulated table.
  SELECT
    *
  FROM tdueck66966.hosts_cumulated
  WHERE DATE = DATE('2022-12-31') --hard-coded date for yesterday that could be parameterized in other tools as Trino appears to be limited.
),
today AS ( --CTE for today's data from the web events dataset joined (left) to populate in the user_devices_cumulated table with the user's browser type.
  SELECT
    host,
    CAST(date_trunc('day', event_time) AS DATE) as event_date,
    COUNT(1)
  FROM bootcamp.web_events
  WHERE date_trunc('day', event_time) = DATE('2023-01-01') --hard-coded date for today that could be parameterized in other tools as Trino appears to be limited.
  GROUP BY host, CAST(date_trunc('day', event_time) AS DATE)
  )
SELECT
  COALESCE(y.host, t.host) AS host, --coalesced columns to capture both yesterday's and today's values when there isn't a change.
  CASE
    WHEN y.host_activity_datelist IS NOT NULL THEN ARRAY[t.event_date] || y.host_activity_datelist
    ELSE ARRAY[t.event_date] --used array and concatenate to capture all web activity per host in a single field
  END AS host_activity_datelist,
  DATE('2023-01-01') AS DATE
FROM
  yesterday y
  FULL OUTER JOIN today t ON y.host = t.host
