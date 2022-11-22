truncate table mekari.salary_period;

insert into mekari.salary_period as
with time_range  as (
	select distinct extract(year from to_date(date,'yyyy-mm-dd')) as year,extract(month from to_date(date,'yyyy-mm-dd')) as month  from timesheets
	order by year, month
), employee_  as (
	SELECT employe_id, branch_id, salary, join_date, case when resign_date='' then '2900-01-01' else resign_date end as resign_date
	FROM (
		select *, row_number() over(partition by employe_id order by salary desc) rnum from 
		employees 
	) a where rnum = 1
), time_salary as (
	select * from time_range a left join employee_ b on to_date(b.join_date,'yyyy-mm-dd')<=to_date(concat(a.year,lpad(a.month::varchar,2,'0'),'28'),'yyyymmdd')
	and to_date(b.resign_date,'yyyy-mm-dd')>to_date(concat(a.year,lpad(a.month::varchar,2,'0'),'01'),'yyyymmdd')
), salary_period as (
	select year::int, month::int, branch_id, sum(salary) from time_salary group by 1,2,3
)select * from salary_period bs order by year, month, branch_id; 