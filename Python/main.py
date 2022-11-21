from csv import DictReader
from datetime import datetime

INPUT_PATH = 'G:/mekari/Mekari Data Engineer/salary_per_hour/Python/input/'
EMPLOYEE_FILE = INPUT_PATH+'employees.csv'
TIMESHEET_FILE = INPUT_PATH+'timesheets.csv'


def read_file(PATH):
    with open(PATH, 'r') as f:\
        dict_reader = DictReader(f)
        data = list(dict_reader)

    return data

#read file
employees = read_file(EMPLOYEE_FILE)
timesheets = read_file(TIMESHEET_FILE)

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
for x in item_2.keys():
    temp = x.split('_')
    key = temp[0]+'_'+temp[1]+'_'+temp[2]
    salary = float(temp[4])
    if key not in branch_salary.keys():
        branch_salary[key] = 0
    branch_salary[key]  += salary
    

##calculating branch salary per hour per period##
result={}
for x in branch_salary.keys():
    result[x]=branch_salary[x]/working_hour[x]

            