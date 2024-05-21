INSERT INTO tdueck66966.user_devices_cumulated
WITH yesterday AS ( --CTE for yesterday's data for user web activity by browser type already populated in the user_devices_cumulated table.
  SELECT
    *
  FROM tdueck66966.user_devices_cumulated
  WHERE DATE = DATE('2022-12-31') --hard-coded date for yesterday that could be parameterized in other tools as Trino appears to be limited.
),
today AS ( --CTE for today's data from the web events dataset joined (left) to populate in the user_devices_cumulated table with the user's browser type.
  SELECT
    we.user_id,
    d.browser_type,
    CAST(date_trunc('day', we.event_time) AS DATE) as event_date,
    COUNT(1)
  FROM bootcamp.web_events we
  LEFT JOIN bootcamp.devices d ON we.device_id = d.device_id --used left join to return all rows from web_events even if the user didn't have data in the devices dataset.
  WHERE
    date_trunc('day', we.event_time) = DATE('2023-01-01') --hard-coded date for today that could be parameterized in other tools as Trino appears to be limited.
  GROUP BY
    we.user_id,
    d.browser_type,
    CAST(date_trunc('day', we.event_time) AS DATE)
  )
  
SELECT
  COALESCE(y.user_id, t.user_id) AS user_id, --coalesced columns to capture both yesterday's and today's values when there isn't a change.
  COALESCE(y.browser_type, t.browser_type) AS browser_type, --coalesced columns to capture both yesterday's and today's values when there isn't a change.
  CASE
    WHEN y.dates_active IS NOT NULL THEN ARRAY[t.event_date] || y.dates_active
    ELSE ARRAY[t.event_date] --utilized array and concatenate to capture all web activity per user per browser_type in a single field.
  END AS dates_active,
  DATE('2023-01-01') AS DATE
FROM
  yesterday y
  FULL OUTER JOIN today t ON y.user_id = t.user_id
