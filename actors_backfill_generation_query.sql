--=======================================
--Backfill Query for actors_history_scd
--=======================================
WITH params AS (
    SELECT 2021 AS up_to_year
),

with_previous AS (
	SELECT
		a.actorid,
		a.actor, 
		a.current_year, 
		a.quality_class, 
		a.is_active, 
	LAG(a.quality_class, 1) OVER(PARTITION BY a.actorid ORDER BY a.current_year) as previous_quality_class, 
	LAG(a.is_active, 1) OVER(PARTITION BY a.actorid ORDER BY a.current_year) as previous_is_active
	FROM actors a
	JOIN params p 
	ON a.current_year <= p.up_to_year
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

INSERT INTO actors_history_scd (
	actorid,
	actor,
	quality_class,
	is_active,
	start_year, 
	end_year,
	current_year)
	
SELECT 
	s.actorid,
	s.actor,
	s.quality_class,
	s.is_active,
	MIN(s.current_year) AS start_year, 
	MAX(s.current_year) AS end_year,
	p.up_to_year AS current_year
FROM with_streaks s 
CROSS JOIN Params P
GROUP BY s.actor,s.actorid, s.streak_identifier, s.is_active, s.quality_class, p.up_to_year


