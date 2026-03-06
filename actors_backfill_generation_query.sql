--=======================================
--Backfill Query for actors_history_scd
--=======================================
WITH params AS (
    SELECT 2021 AS up_to_year
),

WITH with_previous AS (
	SELECT
		actorid,
		actor, 
		current_year, 
		quality_class, 
		is_active, 
	LAG(quality_class, 1) OVER(PARTITION BY actorid ORDER BY current_year) as previous_quality_class, 
	LAG(is_active, 1) OVER(PARTITION BY actorid ORDER BY current_year) as previous_is_active
	FROM actors
	WHERE current_year <= 2021
), 

with_indicators AS (
SELECT
	*,
	CASE 
		WHEN previous_quality_class IS NULL THEN 1
		WHEN quality_class IS DISTINCT FROM previous_quality_class THEN 1 
		WHEN is_active IS DISTINCT FROM previous_is_active THEN 1 
		ELSE 0
	END AS change_indicator
FROM with_previous
),

with_streaks AS (
SELECT
	*,
	SUM(change_indicator) OVER(PARTITION BY actorid ORDER BY current_year) AS streak_identifier
FROM with_indicators
)

INSERT INTO actors_history_scd
SELECT 
	actorid,
	actor,
	quality_class,
	is_active,
	MIN(current_year) AS start_year, 
	MAX(current_year) AS end_year,
	param AS current_year
FROM with_streaks
GROUP BY actor,actorid, streak_identifier, is_active, quality_class


