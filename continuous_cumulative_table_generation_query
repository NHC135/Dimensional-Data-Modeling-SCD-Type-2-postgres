/*===========================================
continuous cumulative table generation query 
=============================================*/

-- the time variant is in years range from 1970 - 2021
-- WARNING: row explosion using continous (generate_series) not incremental load
WITH years AS (
	SELECT * FROM GENERATE_SERIES(1970, 2021) AS year
),

-- here we find the actors debut year
actors AS ( 
	SELECT
		actor,
		actorid,
		MIN(year) AS first_year
	FROM actor_films
	GROUP BY actor, actorid
),

-- this CTE matches the actor and debut year, the time spine
actors_and_years AS (
	SELECT
		a.actor,
		a.actorid,
		y.year
	FROM actors a 
	JOIN years y
	ON a.first_year <= y.year
),

-- 'aggregated' aggregate actors film stats for each year
films_by_year AS (
	SELECT 
		aay.actor, 
		aay.actorid,
		aay.year, 
		ARRAY_AGG(
			ROW(af.film, af.votes, af.rating, af.filmid)::films)
				FILTER(WHERE af.year IS NOT NULL) AS films, 
		AVG(rating) AS avg_rating,
		count(af.filmid) > 0 AS is_active
	FROM actors_and_years aay
	LEFT JOIN actor_films af
	ON aay.actorid = af.actorid
	AND aay.year = af.year
	GROUP BY aay.actor, aay.actorid, aay.year
)

--we use partitions to build cumulative arrays 
	SELECT 
		actor,
		actorid,
		year,  
		films,
		(CASE
			WHEN avg_rating > 8 THEN 'star' 
			WHEN avg_rating > 7 THEN 'good'
			WHEN avg_rating > 6 THEN 'average' 
			ELSE 'bad'
		END)::quality_class AS quality_class,
		is_active
	FROM films_by_year
