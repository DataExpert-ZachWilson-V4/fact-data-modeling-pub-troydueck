CREATE OR REPLACE TABLE tdueck66966.user_devices_cumulated (
  user_id BIGINT, --'user_id': stores the user ID from the web_events dataset.
  browser_type VARCHAR, --'browser_type': stores the browser type associated with the user log from the devices dataset using a left outer join.
  dates_active ARRAY(DATE), --'dates_active': stores the array of event activity from the web_events dataset.
  date DATE --'date': stores the capture date of the user web activity to build a cumulative table.
)
WITH (
  FORMAT = 'PARQUET',
  partitioning = ARRAY['date']
)
