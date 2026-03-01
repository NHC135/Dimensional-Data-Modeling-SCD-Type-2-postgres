/*
Assignment: create DDL for actors tables
*/ 

-- create type array for films ARRAY
CREATE TYPE films AS (
	film TEXT,
	votes INT,
	rating REAL, 
	filmid TEXT
	);
	
-- create type enum 'quality_class' ENUM	
CREATE TYPE quality_class AS 
	ENUM ('star', 'good', 'average', 'bad');


-- ddl for table actors
CREATE TABLE actors ( 
	actor TEXT, 
	actorid TEXT,
	films films[],
	quality_class quality_class,
	is_active BOOLEAN,
	current_year INT, 
	PRIMARY KEY(actorid, current_year)
);
