/*============================
DDL actors_history_scd table
=============================*/

CREATE TABLE IF NOT EXISTS actors_history_scd (
	actorid TEXT NOT NULL,
	actor TEXT NOT NULL,
	quality_class quality_class NOT NULL,
	is_active BOOLEAN NOT NULL,
	start_year INTEGER NOT NULL, 
	end_year INTEGER NOT NULL,
	current_year INTEGER NOT NULL,
	PRIMARY KEY(actorid, start_year, end_year)
); 

