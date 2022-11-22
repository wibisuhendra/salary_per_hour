-- mekari.branch_salary definition

-- Drop table

-- DROP TABLE mekari.branch_salary;

CREATE TABLE mekari.branch_salary (
	branch_id int4 NULL,
	"year" int4 NULL,
	"month" int4 NULL,
	total_work_hour float8 NULL,
	total_salary int8 NULL,
	salary_per_hour float8 NULL
);