/* ==========================================
incremental cumulative table generation query
===========================================*/


-- for incremental we establish a last year & current year
WITH last_year AS (
	SELECT 
	*
	FROM actors
	WHERE current_year = 2021 -- Change this for each year 1970 -2021
), 

current_year AS (
 SELECT 
        actorid,
        actor,
        ARRAY_AGG(
            ROW(
                film,
                votes,
                rating,
                filmid
            )::films
        ) AS films,
        AVG(rating) AS avg_rating
    FROM actor_films
    WHERE year = 2022 -- Change this for each year (e.g., 1970 - 2021)
    GROUP BY actorid, actor
),

cumulative AS (
SELECT 
	COALESCE(ly.actor, cy.actor) AS actor, 
	COALESCE(ly.actorid, cy.actorid) AS actorid, 
	CASE
		WHEN ly.films IS NULL THEN cy.films 
		WHEN cy.films IS NOT NULL THEN ly.films
		ELSE ly.films || cy.films
	END AS films,
	CASE WHEN cy.avg_rating IS NOT NULL THEN 
		(CASE 
			WHEN cy.avg_rating > 8 THEN 'star'
			WHEN cy.avg_rating > 7 THEN 'good'
			WHEN cy.avg_rating > 6 THEN 'average'
			ELSE 'bad'
		END)::quality_class
		ELSE ly.quality_class 
	END AS quality_class, 
	cy.actorid IS NOT NULL AS is_active,
	2022 AS current_year
FROM last_year ly
FULL OUTER JOIN current_year cy 
ON ly.actorid = cy.actorid 
)

INSERT INTO actors
SELECT
*
FROM cumulative
