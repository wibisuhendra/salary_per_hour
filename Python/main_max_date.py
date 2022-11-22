from csv import DictReader
from datetime import datetime
import psycopg2

INPUT_PATH = 'G:/mekari/Mekari Data Engineer/salary_per_hour/Python/input/'
EMPLOYEE_FILE = INPUT_PATH+'employees.csv'
TIMESHEET_FILE = INPUT_PATH+'timesheets.csv'
DB_NAME='postgres'
DB_USER='postgres'
DB_PASSWD=''
DB_HOST='localhost'
DB_PORT='5432'

DB={}
DB['DB_NAME']=DB_NAME
DB['DB_USER']=DB_USER
DB['DB_PASSWD']=DB_PASSWD
DB['DB_HOST']=DB_HOST
DB['DB_PORT']=DB_PORT


def read_file(PATH):
    with open(PATH, 'r') as f:
        dict_reader = DictReader(f)
        data = list(dict_reader)

    return data


#notes: assumed there is already staging table for each data

#connect db function
def db_connect():
    conn = psycopg2.connect(
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWD,
        host=DB_HOST,
        port=DB_PORT
    )
    return conn

#close connection function
def conn_close(conn):
    conn.close()

#function for execute select data from query
def query_get(query):
    conn = db_connect()
    with conn:
        with conn.cursor() as cur:
            cur.execute(query)
            result = cur.fetchall()
    conn_close(conn)
    
    return result

#function for execute query w/o return value
def query_exec(query):
    conn = db_connect()
    conn.autocommit = True
    with conn:
        with conn.cursor() as cur:
            cur.execute(query)
    conn_close(conn)

#function for insert employee data
def insert_employees(data):
    conn = db_connect()
    conn.autocommit = True
    for item in data:
        attr=list(item.values())
        insert_item = "'"+str(attr[0])+"'"
        for val in attr:
            if val != attr[0]:
                insert_item += ",'"+ str(val)+"'"

        query_insert='''insert into mekari.employees (employe_id,branch_id,salary,join_date,resign_date) 
        VALUES ({});'''.format(insert_item)

        with conn:
            with conn.cursor() as cur:
                cur.execute(query_insert)
    conn_close(conn)

#function for incrementing Employees data based on date 
def increment_employees_data():
    #get existing max date
    query_employee = '''select max(to_date(join_date,'yyyy-mm-dd')) from mekari.employees'''
    max_date_employee = query_get(query_employee)[0][0]
    if max_date_employee == None:
        max_date_employee = datetime.strptime('1900-01-01', '%Y-%m-%d').date()
    employees_ = read_file(EMPLOYEE_FILE)
    employees = []

    for employee in employees_:
        #get only new data
        if datetime.strptime(employee['join_date'], '%Y-%m-%d').date() > max_date_employee:
            employees.append(employee)
    if len(employees) !=0:
        insert_employees(employees)

#function for getting employees data
def get_employee_data():
    query = '''select employe_id, branch_id, salary, join_date, resign_date from (
        select *, row_number() over(partition by employe_id order by salary desc) rnum from 
        mekari.employees 
        ) a where rnum = 1'''
    query_result = query_get(query)
    keys = ['employe_id', 'branch_id', 'salary', 'join_date', 'resign_date']
    result = [dict(zip(keys, values)) for values in query_result]
    
    
    return result

#function for insert into timesheets table
def insert_timesheets(data):
    conn = db_connect()
    cur=conn.cursor() 
    conn.autocommit = True
    for item in data:
        attr=list(item.values())
        insert_item = "'"+str(attr[0])+"'"
        for val in attr:
            if val != attr[0]:
                insert_item += ",'"+ str(val)+"'"

        query_insert='''insert into mekari.timesheets (timesheet_id, employee_id, "date", checkin, checkout) 
        VALUES ({});'''.format(insert_item)
        cur.execute(query_insert)
    conn_close(conn)


#function for incrementing timesheets data
def incremental_timesheets_data():
    #get existing max date
    query = '''select coalesce(max(to_date(date,'yyyy-mm-dd')),to_date('1900-01-01','yyyy-mm-dd')) 
        from mekari.timesheets'''
    max_date = query_get(query)[0][0]
    timesheets_data = read_file(TIMESHEET_FILE)
    timesheets = []

    for timesheet in timesheets_data:
        #take only new data
        #take the whole month because salary per branch per month is based on timesheet data
        if datetime.strptime(timesheet['date'], '%Y-%m-%d').date() >= max_date.replace(day=1):
            timesheets.append(timesheet)
    if len(timesheets) !=0:
        #insert 
        insert_timesheets(timesheets)

    #return delta
    return timesheets

#function for inserting salary per month
def insert_salary_per_hour(data):
    columns = ['branch_id', "year", "month", 'total_work_hour', 'total_salary', 'salary_per_hour']
    #get existing that match with delta
    query='''select branch_id, "year", "month", total_work_hour, total_salary, salary_per_hour
        from mekari.branch_salary where  branch_id = {branch_id} and year = '{year}'
        and month = '{month}' '''.format(branch_id =data['branch_id'],year=data['year'], month=data['month'])
    query_result = query_get(query)
    result = [dict(zip(columns, values)) for values in query_result]

    #delete if branch_period exist
    query_delete='''delete from mekari.branch_salary where branch_id = {branch_id} and year = {year}
        and month = {month}'''.format(branch_id =data['branch_id'],year=data['year'], month=data['month'])

    #insert the new data (delta)
    query_insert = '''INSERT INTO mekari.branch_salary
        (branch_id, "year", "month", total_work_hour, total_salary, salary_per_hour)
        VALUES({}, {},{}, {}, {}, {});
        '''.format(data['branch_id'],data['year'], data['month'],data['total_work_hour'],data['total_salary'],
                  data['salary_per_hour'])
    
    query_exec(query_delete)
    query_exec(query_insert)
    


increment_employees_data()
employees_ = get_employee_data()
timesheets_ = incremental_timesheets_data()


timesheets = timesheets_.copy()
employees = employees_.copy()

##calculating work_hour of timesheet##
for timesheet in timesheets:
    #if checkout time or checkin time empty, work_hour assumed as 8 hours
    if timesheet['checkout'] == '' or timesheet['checkin'] == '':
        timesheet['work_hour'] = 8
    else:
        checkin = datetime.strptime(timesheet['date']+' '+timesheet['checkin'], '%Y-%m-%d %H:%M:%S').timestamp()
        checkout = datetime.strptime(timesheet['date']+' '+timesheet['checkout'], '%Y-%m-%d %H:%M:%S').timestamp()
        #if checkout time smaller or equal than checkin time, work_hour also assumed as 8 hours
        if checkin >= checkout:
            timesheet['work_hour'] = 8
        else:
            timesheet['work_hour'] = (checkout-checkin)/3600


##lookup branch_id, employee_id, and salary from employees data for timesheets data (with work_hour)##
joined = []
for item in timesheets:
    temp = {}
    counter = 0
    temp = item
    found=0
    while found == 0 and counter < len(employees):
        if str(item['employee_id'])==str(employees[counter]['employe_id']):
            found=1
            item['branch_id'] = employees[counter]['branch_id']
            item['salary'] = employees[counter]['salary']
        counter+=1
    joined.append(temp)


##summarize working hour
working_hour = {}
for timesheet in timesheets:
    date_split = timesheet['date'].split('-')
    key = str(timesheet['branch_id'])+'_'+date_split[0]+'_'+date_split[1] #keys for grouping 
    if key not in working_hour.keys():
        working_hour[key] = 0
    working_hour[key]  = working_hour[key]+ timesheet['work_hour']


##distinguish branch, periods, employee, and salary for salary calculation##
branch_employee = {}
for timesheet in timesheets:
    date_split = timesheet['date'].split('-')
    key = str(timesheet['branch_id'])+'_'+str(date_split[0])+'_'+str(date_split[1])+'_'+str(timesheet['employee_id'])+'_'+str(timesheet['salary']) #keys for grouping 
    if key not in branch_employee.keys():
        branch_employee[key] = 0
    branch_employee[key]  += 1


##calculating branch salary outcome per period## 
branch_salary = {}
for x in branch_employee.keys():
    temp = x.split('_')
    key = temp[0]+'_'+temp[1]+'_'+temp[2]
    salary = float(temp[4])
    if key not in branch_salary.keys():
        branch_salary[key] = 0
    branch_salary[key]  += salary


##calculating branch salary per hour per period##
result=[]
for x in branch_salary.keys():
    temp={}
    temp['branch_id']=x.split('_')[0]
    temp['year']=x.split('_')[1]
    temp['month']=x.split('_')[2]
    temp['total_work_hour']=working_hour[x]
    temp['total_salary']=branch_salary[x]
    temp['salary_per_hour']=branch_salary[x]/working_hour[x]
    result.append(temp)

#insert the result of calculation to destionation table
for item in result:
    insert_salary_per_hour(item)