WITH today AS (
  SELECT
    *
  FROM tdueck66966.user_devices_cumulated
  WHERE DATE = DATE('2023-01-07')
),
date_list_int AS (
  SELECT
    user_id,
    browser_type,
    CAST( SUM(
      CASE WHEN CONTAINS(dates_active, sequence_date) --evaluates when the sequence date is contained in the array
        THEN POW(2, 31 - DATE_DIFF('day', sequence_date, DATE)) --multiples the difference of the sequence date contained in the array from today (the most recent sequence date) by the power of 2 (^2) after subtracting 31 to convert the value into a base-2 integer
        ELSE 0 END) AS BIGINT --sums the above base-2 integers to provide the integer of activity history
      ) AS history_int
    FROM today
    CROSS JOIN UNNEST (SEQUENCE(DATE('2023-01-01'), DATE('2023-01-07'))) AS t (sequence_date) --the cross join provides all values from the dates_active array by today (the most recent date evaluated)
    GROUP BY user_id, browser_type
)
SELECT
  *,
  TO_BASE(history_int, 2) as history_in_binary --converts the integer that contains all the activity history for the user by browser type into a binary display that can be used to quickly determine users' activity by this_week, first 3 days, last_week, etc. There is a remaining issue of leading 0's being dropped from the TO_BASE function that needs corrected for these analyses to work properly
FROM date_list_int
