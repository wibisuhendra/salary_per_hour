truncate table mekari.branch_salary

insert into mekari.branch_salary
with timesheet_cleansing as (
	select timesheet_id,employee_id, to_date(date,'yyyy-mm-dd') as date, 
	to_timestamp(concat(date,' ',checkin),'yyyy-mm-dd hh24:mi:ss')::timestamp as checkin,
 	to_timestamp(concat(date,' ',checkout),'yyyy-mm-dd hh24:mi:ss')::timestamp as checkout 
 	from mekari.timesheets t  where checkout != '' and checkin !=''
 	union all 
 	select timesheet_id, employee_id, to_date(date,'yyyy-mm-dd'), to_timestamp(concat(date,' ',checkin),'yyyy-mm-dd hh24:mi:ss')::timestamp checkin, null as checkout
	from mekari.timesheets t  where checkout = '' 
	union all 
	select timesheet_id, employee_id, to_date(date,'yyyy-mm-dd'), null as checkin, to_timestamp(concat(date,' ',checkout),'yyyy-mm-dd hh24:mi:ss')::timestamp checkout
	from mekari.timesheets t  where checkin = '' 
), timesheet_bad as ( 
	select * from timesheet_cleansing where checkin is null
 	union all 
 	select * from timesheet_cleansing where checkout is null
 	union all 
 	select * from timesheet_cleansing where checkout is not null and checkout is not null and checkout <= checkin 
), timesheet_good as (
	select * from timesheet_cleansing where checkout is not null and checkout is not null and checkout  > checkin 
), hour_good as (
	select *, extract(EPOCH from (checkout - checkin))/3600  as work_hour from timesheet_good 
), hour_bad as (
	select *, 8 as work_hour from timesheet_bad
), timesheet_hour as (
	select * from  hour_bad union all select * from hour_good
), employees_cleansing as (
	select employe_id, branch_id, salary, join_date, resign_date from (
	select *, row_number() over(partition by employe_id order by salary desc) rnum from 
	employees ) a where rnum = 1
), timesheet_employee as (
	select a.timesheet_id, a.employee_id, b.branch_id, b.salary ,a.date, extract(year from a.date) as  year, extract(month from a.date) as month,  a.checkin, a.checkout, a.work_hour 
	from timesheet_hour a left join employees_cleansing b
	on a.employee_id = b.employe_id 
), timesheet_work_hour as (
	select branch_id, year, month, sum(work_hour) as total_work_hour from timesheet_employee group by 1,2,3 
), timesheet_branch as (
	 select distinct branch_id, year, month, employee_id, salary  from timesheet_employee
), branch_salary as (
	select branch_id,year,month, sum(salary) total_salary from timesheet_branch group by 1,2,3
) select a.branch_id, a.year, a.month, a.total_work_hour, b.total_salary, b.total_salary/a.total_work_hour salary_per_hour from timesheet_work_hour a left join 
branch_salary b on a.branch_id = b.branch_id and a.year = b.year and a.month = b.month
