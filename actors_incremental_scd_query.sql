/* ========================================
Incremental query for actors_history_scd
======================================== */
/*
CREATE TYPE scd_type AS (
                    quality_class quality_class,
                    is_active boolean,
                    start_year INTEGER,
                    end_year INTEGER
                        )
*/

WITH last_year_data AS (
	SELECT * FROM actors_history_scd 
	WHERE current_year = 2020
	AND end_year = 2020
), 

historical_scd AS ( 
	SELECT 
		actorid,
		actor,
		quality_class,
		is_active, 
		start_year, 
		end_year
	FROM actors_history_scd 
	WHERE current_year = 2020
	and end_year < 2020
),
	
this_year_data AS ( 
	SELECT * FROM actors
	WHERE current_year = 2021
),

unchanged_records AS ( 
	SELECT 
		ty.actorid,
		ty.actor, 
		ty.quality_class, 
		ty.is_active,
		ly.start_year,
		ty.current_year AS end_year
	FROM this_year_data ty
	LEFT JOIN last_year_data ly
	ON ty.actorid = ly.actorid 
	WHERE ty.quality_class = ly.quality_class 
	AND ty.is_active = ly.is_active
),

changed_records AS (
SELECT 
	ty.actorid,
	ty.actor, 
	UNNEST(ARRAY[
		ROW(ly.quality_class, 
			ly.is_active, 
			ly.start_year, 
			ly.end_year
			)::scd_type,
		ROW(ty.quality_class, 
			ty.is_active, 
			ty.current_year, 
			ty.current_year
			)::scd_type
	]) AS records
FROM this_year_data ty
LEFT JOIN last_year_data ly
ON ty.actorid = ly.actorid
WHERE (ty.quality_class <> ly.quality_class
	OR ty.is_active <> ly.is_active)
),

unnested_changed_records AS ( 
	SELECT 
		actorid,
		actor, 
		(records::scd_type).quality_class,
		(records::scd_type).is_active,
		(records::scd_type).start_year,
		(records::scd_type).end_year
	FROM changed_records
),

new_records AS (
	SELECT 
		ty.actorid,
		ty.actor,
		ty.quality_class, 
		ty.is_active,
		ty.current_year AS start_year,
		ty.current_year AS end_year 
	FROM this_year_data ty
	LEFT JOIN last_year_data ly
	ON ty.actorid = ly.actorid
	WHERE ly.actorid IS NULL
)


SELECT *, 2021 AS current_year FROM (
                  SELECT *
                  FROM historical_scd

                  UNION ALL

                  SELECT *
                  FROM unchanged_records

                  UNION ALL

                  SELECT *
                  FROM unnested_changed_records

                  UNION ALL

                  SELECT *
                  FROM new_records
              ) final;

