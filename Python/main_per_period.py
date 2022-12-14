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

CURRENT_TIME = datetime.now() #for increment
YEAR = str(CURRENT_TIME.year)
if CURRENT_TIME.month < 10:
    MONTH = '0'+str(CURRENT_TIME.month)
else:
    MONTH = str(CURRENT_TIME.month)

def read_file(PATH):
    with open(PATH, 'r') as f:
        dict_reader = DictReader(f)
        data = list(dict_reader)

    return data

#!!!!FOR MANUAL OVERRIDE!!!!
# MONTH = '08'
# YEAR = '2019'

#read file
employees = read_file(EMPLOYEE_FILE)
timesheets_ = read_file(TIMESHEET_FILE)

#filter current period data only
timesheets = []
for timesheet in timesheets_:
    data_date = timesheet['date'].split('-')
    if data_date[0] == YEAR and data_date[1] == MONTH:
        timesheets.append(timesheet)

#check if there is new data
do = False
if len(timesheets) != 0:
    do = True

if do:
    ##removing duplicate employee##
    #count existing employe_id
    count = {}
    for employee in employees:
        if employee['employe_id'] not in count.keys():
            count[employee['employe_id']] = 0
        count[employee['employe_id']] += 1

    #get employe_id having count more than one
    duplicate=[]
    for i in count:
        if count[i] >1:
            duplicate.append(i)

    #handling duplicate data
    for item in duplicate:
        duplicate_employee =[]
        for employee in employees:
            if employee['employe_id'] == item:  #get employee data by employe_id
                duplicate_employee.append(employee)

        employee_ = max(duplicate_employee, key=lambda x:x['salary']) #only take employee data with highest salary employee_id
        for i in duplicate_employee:
            employees.remove(i) #remove the duplicate employee
        employees.append(employee_) #rewrite with data having highest salary


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
            employees[counter]
            if item['employee_id']==employees[counter]['employe_id']:
                found=1
                temp['branch_id'] = employees[counter]['branch_id']
                temp['salary'] = employees[counter]['salary']
            counter+=1
        joined.append(temp)


    ##summarize working hour
    working_hour = {}
    for timesheet in timesheets:
        date_split = timesheet['date'].split('-')
        key = timesheet['branch_id']+'_'+date_split[0]+'_'+date_split[1] #keys for grouping 
        if key not in working_hour.keys():
            working_hour[key] = 0
        working_hour[key]  = working_hour[key]+ timesheet['work_hour']


    ##distinguish branch, periods, employee, and salary for salary calculation##
    branch_employee = {}
    for timesheet in timesheets:
        date_split = timesheet['date'].split('-')
        key = timesheet['branch_id']+'_'+date_split[0]+'_'+date_split[1]+'_'+timesheet['employee_id']+'_'+timesheet['salary'] #keys for grouping 
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
        
    ##insert data to destination table##
 
        conn = psycopg2.connect(
           database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWD,
            host=DB_HOST,
            port=DB_PORT
        )

        # delete current period (year-month) data
        query_delete = '''delete from mekari.branch_salary where year={year} and month={month}'''.format(year=YEAR, month=MONTH)
        
        with conn:
            with conn.cursor() as cur:
                cur.execute(query_delete)

        # insert data to db
        for item in result:
            attr=list(item.values())
            insert_item = str(attr[0])
            for val in attr:
                if val != attr[0]:
                    insert_item += ','+ str(val)

            query_insert='''insert into mekari.branch_salary (branch_id, year, month,total_work_hour, total_salary, salary_per_hour) 
            VALUES ({});'''.format(insert_item)

            with conn:
                with conn.cursor() as cur:
                    cur.execute(query_insert)
        conn.close()
else:
    print('no new data')
            