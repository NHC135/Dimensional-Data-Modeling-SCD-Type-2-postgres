/* ==========================================
incremental cumulative table generation query
===========================================*/

WITH params AS (
    SELECT
        2020 AS last_year,
        2021 AS current_year
),

last_year AS (
	SELECT
		a.*
	FROM actors a,
	JOIN params p
	WHERE a.current_year = p.last_year
),

current_year AS (
    SELECT 
        af.actorid,
        af.actor,
        ARRAY_AGG(
            ROW(
                af.film,
                af.votes,
                af.rating,
                af.filmid
            )::films
            ORDER BY filmid
        ) AS films,
        AVG(rating) AS avg_rating
    FROM actor_films af 
	JOIN params p
	ON af.year = p.current_year
    GROUP BY af.actorid, af.actor
),

cumulative AS (
SELECT 
	COALESCE(ly.actor, cy.actor) AS actor,
	COALESCE(ly.actorid, cy.actorid) AS actorid,
	CASE
		WHEN ly.films IS NULL THEN cy.films
		WHEN cy.films IS NULL THEN ly.films
		ELSE ly.films || cy.films
	END AS films,

	CASE 
		WHEN cy.avg_rating IS NOT NULL THEN
			CASE 
				WHEN cy.avg_rating > 8 THEN 'star'
				WHEN cy.avg_rating > 7 THEN 'good'
				WHEN cy.avg_rating > 6 THEN 'average'
				ELSE 'bad'
			END::quality_class
		ELSE ly.quality_class
	END AS quality_class,
	cy.actorid IS NOT NULL AS is_active,
	p.current_year AS current_year
FROM last_year ly 
FULL OUTER JOIN current_year cy
	ON ly.actorid = cy.actorid
CROSS JOIN params
)

INSERT INTO actors
(actor, actorid, films, quality_class, is_active, current_year)
SELECT 
	actor, 
	actorid,
	films,
	quality_class,
	is_active,
	current_year
FROM cumulative;
