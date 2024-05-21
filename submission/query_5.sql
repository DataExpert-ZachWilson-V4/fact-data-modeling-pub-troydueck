CREATE OR REPLACE TABLE tdueck66966.hosts_cumulated (
  host VARCHAR, --'host': stores the host from the web_events dataset.
  host_activity_datelist ARRAY(DATE), --'host_activity_datelist': stores the array of event activity for the host from the web_events dataset.
  date DATE --'date': stores the capture date of the user web activity to build a cumulative table
)
WITH (
  FORMAT = 'PARQUET',
  partitioning = ARRAY['date']
)
