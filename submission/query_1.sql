WITH dedup AS (
  SELECT
    *, --For queries in prod, I would call the specific columns in the CTE for robustness to guard against new columns causing downstream pipeline issues. I've left it * here for brevity.
    ROW_NUMBER() OVER (PARTITION BY game_id, team_id, player_id) as rownum --This function iterates a sequential index by the partition, which can be used to find the first row for a group of values.
  FROM bootcamp.nba_game_details
)
SELECT
  *
FROM dedup
WHERE rownum = 1 --Filters the query to only return the first row of the given partition of the CTE to dedup the handful of duplicated players per game. Original dataset was 668,628 rows and the deduped dataset is 668,339 rows
