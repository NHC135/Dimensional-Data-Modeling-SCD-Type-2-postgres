/*
Assignment: create DDL for actors tables
*/ 

-- create type array for films ARRAY
CREATE TYPE IF NOT EXISTS films AS (
	film TEXT,
	votes INT,
	rating REAL, 
	filmid TEXT
	);
	
-- create type enum 'quality_class' ENUM	
CREATE TYPE IF NOT EXISTS quality_class AS 
	ENUM ('star', 'good', 'average', 'bad');


-- ddl for table actors
CREATE TABLE IF NOT EXISTS actors ( 
	actor TEXT NOT NULL, 
	actorid TEXT NOT NULL,
	films films[],
	quality_class quality_class,
	is_active BOOLEAN NOT NULL,
	current_year INT NOT NULL, 
	PRIMARY KEY(actorid, current_year)
);

-- Insert a single actor record with explicit column list
INSERT INTO actors (actor, actorid, films, quality_class, is_active, current_year)
VALUES (
    'Tom Hanks',
    'nm0000158',
    ARRAY[
        ROW('Forrest Gump', 678815, 8.8, 'tt0109830')::films,
        ROW('Cast Away', 235040, 7.8, 'tt0162222')::films
    ],
    'star',
    TRUE,
    2024
);
