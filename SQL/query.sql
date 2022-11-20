truncate table mekari.branch_salary;

insert into mekari.branch_salary
with timesheet_cleansing as ( --cleansing '' data to null and fix data type
	select timesheet_id,employee_id, to_date(date,'yyyy-mm-dd') as date, 
		to_timestamp(concat(date,' ',checkin),'yyyy-mm-dd hh24:mi:ss')::timestamp as checkin,
 		to_timestamp(concat(date,' ',checkout),'yyyy-mm-dd hh24:mi:ss')::timestamp as checkout 
 	from mekari.timesheets t  where checkout != '' and checkin !=''
 	union all 
 	select timesheet_id, employee_id, to_date(date,'yyyy-mm-dd'), 
 		to_timestamp(concat(date,' ',checkin),'yyyy-mm-dd hh24:mi:ss')::timestamp checkin, null as checkout
	from mekari.timesheets t  where checkout = '' 
	union all 
	select timesheet_id, employee_id, to_date(date,'yyyy-mm-dd'), null as checkin, 
		to_timestamp(concat(date,' ',checkout),'yyyy-mm-dd hh24:mi:ss')::timestamp checkout
	from mekari.timesheets t  where checkin = '' 
), timesheet_bad as ( --timesheet data with bad form, that are data having checkin null, checkout null, or checkout <= checkin
	select * from timesheet_cleansing where checkin is null
 	union all 
 	select * from timesheet_cleansing where checkout is null
 	union all 
 	select * from timesheet_cleansing where checkout is not null and checkout is not null and checkout <= checkin 
), timesheet_good as ( --timesheet data with good form
	select * from timesheet_cleansing where checkout is not null and checkout is not null and checkout  > checkin 
), hour_good as ( --calculating work_hour for good data
	select *, extract(EPOCH from (checkout - checkin))/3600  as work_hour from timesheet_good 
), hour_bad as ( --work hour assumed as 8 hour for bad data
	select *, 8 as work_hour from timesheet_bad
), timesheet_hour as ( --union the good and the bad data
	select * from  hour_bad union all select * from hour_good
), employees_cleansing as ( --cleansing the double employee data, only take the higher salary
	select employe_id, branch_id, salary, join_date, resign_date from (
		select *, row_number() over(partition by employe_id order by salary desc) rnum from 
		employees 
	) a where rnum = 1
), timesheet_employee as ( --joining timesheet data (with total hour) with employee data
	select a.timesheet_id, a.employee_id, b.branch_id, b.salary ,a.date, extract(year from a.date) as  year, extract(month from a.date) as month,  a.checkin, a.checkout, a.work_hour 
	from timesheet_hour a left join employees_cleansing b
	on a.employee_id = b.employe_id 
), timesheet_work_hour as ( --summarize working hour per year per month and per branch_id
	select branch_id, year, month, sum(work_hour) as total_work_hour from timesheet_employee group by 1,2,3 
), timesheet_branch as ( --base data for calculating branch outcome permonth
	 select distinct branch_id, year, month, employee_id, salary  from timesheet_employee
), branch_salary as ( --calculating branch outcome per month 
	select branch_id,year,month,count(employee_id) total_employee, sum(salary) total_salary from timesheet_branch group by 1,2,3
)
select a.branch_id, a.year, a.month, a.total_work_hour, b.total_salary, b.total_salary/a.total_work_hour salary_per_hour from timesheet_work_hour a left join 
branch_salary b on a.branch_id = b.branch_id and a.year = b.year and a.month = b.month; --result