import pymysql
import timeit
connection = pymysql.connect(host ='localhost', port = 19321, user = 'root', password = 'Sandhya100!', db = 'hw3', cursorclass = pymysql.cursors.DictCursor)
cursor = connection.cursor();

createEmployee = "create table Emp(ID varchar(20), Name varchar(20), Salary int, Manager varchar(20), Department varchar(20));"

cursor.execute(createEmployee)
cursor.commit()

new_list = []

starttime = timeit.timeit()
with open('/home/kmm1110/mysql/data/emp.txt','r') as f:
	for line in f.readlines():
		line = line.strip().split('|')
        cursor.execute("INSERT INTO Emp(ID, Name, Salary, Manager, Department)" "VALUES(%s,%s,%s,%s)", line)
        cursor.commit()
		
endtime = timeit.timeit()


createEmployee1 = "create table Emp1(ID varchar(20), Name varchar(20), Salary int, Manager varchar(20), Department varchar(20));"

cursor.execute(createEmployee1)
cursor.commit()
createIndex = "create index IndexEmp on Emp1(ID, Department);"
cursor.execute(createIndex)
cursor.commit()

starttime_index = timeit.timeit()
with open('/home/kmm1110/mysql/data/emp.txt','r') as f:
	for line in f.readlines():
		line = line.strip().split('|')
		ID,Name,Salary,Manager,Department = int(line[0]),line[1], int(line[2]), int(line[3]),line[4]
		if ID not in new_list:
			insertEmployee1 = "INSERT INTO Emp1(ID,Name,Salary,Manager,Department) Values((),'()',(),(),'()')".format(ID,Name,Salary,Manager,Department)
			new_list.append(ID)
			cursor.execute(insertEmployee1)
                cursor.commit()

endtime_index = timeit.timeit()

connection.close()

print('Total Time taken without index is: ',endtime - starttime)
print('Total Time taken with index is: ',endtime_index - starttime_index)
