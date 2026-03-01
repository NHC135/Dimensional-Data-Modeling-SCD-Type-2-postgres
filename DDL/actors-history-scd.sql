/*============================
DDL actors_history_scd table
=============================*/

CREATE TABLE actors_history_scd (
	actorid TEXT,
	actor TEXT,
	quality_class quality_class,
	is_active BOOLEAN,
	start_year INTEGER, 
	end_year INTEGER,
	current_year INTEGER,
	PRIMARY KEY(actorid, start_year, end_year)
);
