import re
import numpy as np
from numpy import *
from BTrees.OOBTree import OOBTreePy
import csv
import time
import sys

outputFile = open("output.txt",'w')

R = np.ndarray([])
S = np.ndarray([])
temp = np.ndarray([])
columnNames = {}
btree1 = {}
hashT = {}
#columnData = {}
columns = []
#table = {}
logicalOperators = {'or' : '|', 'and' : '&'}
columnTypes = []
R1 = np.ndarray([])

def insert(fileName):   
    global R
    global S                                            
    f = open("D:\DB HW\sales1.txt",'r')                             #change path to fileName    
    firstLine = f.readline().strip().split('|')
    i = 0
    for line in firstLine:
        columnNames[line] = i
        columns.append(line)
        i+=1
    if 'sales1' in fileName:
        R = np.genfromtxt("D:\DB HW\sales1.txt", dtype = None, delimiter = '|', names = True, encoding='utf8')
        print(type(R))
    if 'sales2' in fileName:
        S = np.genfromtxt("D:\DB HW\sales2.txt", dtype = None, delimiter = '|', names = True, encoding='utf8')                                         
    return R, S
def select(resultTable, table, parameters):                         
    global columns  
    global R1  
    startTime = time.time()                                                 
    if ('and' in parameters) or ('or' in parameters):                          
        logicalOperator = re.search('\).+\(', parameters).group(0)[1:-1].strip()
        parameter1 = parameters.split(logicalOperator)[0].strip().replace('(','').replace(')','')
        parameter2 = parameters.split(logicalOperator)[1].strip().replace('(','').replace(')','')
        relop1 = re.search('[\=\!=\>\>=\<\<=]',parameter1).group(0)
        relop2 = re.search('[\=\!=\>\>=\<\<=]',parameter2).group(0)
        column1 = parameter1.split(relop1)[0].strip()
        arithopConst1 = parameter1.split(relop1)[1].strip()
        column2 = parameter2.split(relop2)[0].strip()
        arithopConst2 = parameter2.split(relop2)[1].strip()
        tableName = table
        #R1 = np.where((R[column1] > arithopConst1) | (R[column2] < arithopConst2))
        finalParam = '('+tableName+'['+'"'+(columns[columnNames[column1]])+'"'+"]"+relop1+arithopConst1+')'+logicalOperators[logicalOperator]+'('+tableName+"["+'"'+columns[columnNames[column2]]+'"'+"]"+relop2+arithopConst2+')'
        R1 =  eval(tableName+'['+'np.where'+'('+finalParam+')'+']')
        #dataBaseNames[resultTable] = temp               
        #shape[resultTable] = temp.shape
        temp = R1                 
        print(R1)
        outputFile.write("R1+\n")
        outputFile.write(R1+'\n')
        #print(type(R1))
        return temp
    
    else:
        if resultTable == 'Q2':
            Q2 = Btreesearch(btree1,'5',table)
            temp = Q2
            outputFile.write("Q2+\n")
            outputFile.write(Q2 +'\n')
        elif resultTable == 'Q4':
            Q4 = Hashsearch(hashT, table, '7')
            temp = Q4
            outputFile.write("Q4+\n")
            outputFile.write(Q4+'\n')
        else:
            relop = re.search('[\=\!=\>\>=\<\<=]',parameters).group(0)
            column = parameters.split(relop)[0].strip()
            arithopConst = parameters.split(relop)[1].strip()
            tableName = table
            if relop == '=':
                relop = '=='
            finalParam = '('+tableName+'['+'"'+(columns[columnNames[column]])+'"'+"]"+relop+arithopConst+')'
            temp = eval(tableName+'['+'np.where'+'('+finalParam+')'+']')
            #temp = R[np.where(R['qty'] == 5)]
            if resultTable == 'Q1':
                Q1 = temp
                outputFile.write("Q1+\n")
                outputFile.write(Q1+'\n')
            if resultTable == 'Q3':
                Q3 = temp
                outputFile.write("Q3+\n")
                outputFile.write(Q3+'\n')
            print(temp)
    return temp
    

def project(resultTable, table, parameters):            #the project query is computed by this function, taking input as resultTable, table and parameters just as previous function
    temp = table                                #taking the array stored at dict(key) where key is table                 #reshaping the array to make it compatible
    totalParams = parameters.split(',')
    column_index = []
    newParams = []
    for element in totalParams:
        element = element.strip()
        newParams.append(element)
    #newParams = newParams.strip(',')
    #for columns in newParams:
        #columnstr = str(columnNames[columns] + ',')
    R2 = R1[newParams]
    #R3 = eval(temp+"["+newParams+"]")
    print(R2)
    outputFile.write("R2+\n")
    outputFile.write(R2+'\n')

def avg(resultTable, table, parameters):                #compute the average query 
    #parameters.replace("'","")
    temp = table
    #print(temp.dtype)
    index = columnNames[parameters]
    average = np.average(index)
    print(average)
    outputFile.write("R3+\n")
    outputFile.write(average+'\n')

def sumgroup(resultTable, table, parameters):
    temp = table
    column1 = parameters.split(',')[0]
    column2 = parameters.split(',')[1].strip()
    sumTime = R1[column1]
    arg2 = R1[column2]
    groupby = np.unique(R1[column2])
    sum = 0
    for i in groupby:
        index = np.where(arg2 == i)
        sum = sum+int(sumTime[index])
    R4 = sum
    outputFile.write("R4+\n")
    outputFile.write(R4+'\n')


def avggroup(resultTable, table, parameters):
    temp = table
    column1 = parameters.split(',')[0]
    column2 = parameters.split(',')[1].strip()
    sumQty = R1[column1]
    priceRange = R1[column2]
    groupby = np.unique(R1[column2])
    sum = 0
    avg = 0
    j = 0
    for i in groupby:
        index = np.where(priceRange == i)
        sum = sum+int(sumQty[index])
        j+=1
    avg = sum/j
    R6 = avg
    outputFile.write("R6+\n")
    outputFile.write(R6+'\n')


def movsum(resultTable, table, parameters):
    movingColumn = parameters.split(',')[0]
    no_of_items = parameters.split(',')[0]
    sumArray = np.cumsum(movingColumn)
    zerosArray = np.zeros(movingColumn.size)
    for i in range(0,no_of_items):
        zerosArray[i] = sumArray[i]

    for i in range(0,movingColumn.size):
        if i>= no_of_items:
            zerosArray[i] = movingColumn[i]-movingColumn[i-no_of_items]
    outputFile.write("T4+\n")
    outputFile.write(zerosArray+'\n')


def movavg(resultTable, table, parameters):
    movingColumn = parameters.split(',')[0]
    no_of_items = parameters.split(',')[0]
    sumArray = np.cumsum(movingColumn)
    zerosArray = np.zeros(movingColumn.size)
    for i in range(0,no_of_items):
        zerosArray[i] = sumArray[i]

    for i in range(0,movingColumn.size):
        if i>= no_of_items:
            zerosArray[i] = movingColumn[i]-movingColumn[i-no_of_items]
    outputFile.write("T3+\n")
    outputFile.write(zerosArray+'\n')

def hashDB(arguments):
    stringMatch = re.search('\(.+\)',arguments).group(0)[1:-1]
    table = stringMatch.split(',')[0]
    columnName = stringMatch.strip(',')[2:].strip()
    hashTable = {}
    B1 = eval(table+'['+ '"'+(columnName)+'"'+']')
    index = 0
    for i in B1:
        key = str(i).strip()
        index += 1
        hashTable[key] = index
    return hashTable


def Hashsearch(hashTable, dataTable, hkey):
    searchIndex = hashTable[hkey]
    searchIndex = searchIndex - 1
    returnValue = eval(dataTable+'['+str(searchIndex)+']')
    return returnValue[1]
    outputFile.write("Q4+\n")
    outputFile.write(returnValue[1]+'\n')
    

def Btree(arguments):
    stringMatch = re.search('\(.+\)',arguments).group(0)[1:-1]
    table = stringMatch.split(',')[0]
    columnName = stringMatch.strip(',')[3:].strip()
    B1 = eval(table+'['+ '"'+(columnName)+'"'+']')
    btree = OOBTreePy()
    index = 0
    for i in B1:
        key = str(i).strip()
        index += 1
        btree[key] = index
    return btree

def Btreesearch(btree, key, table):
        searchIndex = btree[key]
        searchIndex = searchIndex - 1
        returnValue = eval(table+'['+str(searchIndex)+']')
        return returnValue[1]
        outputFile.write("Q2+\n")
        outputFile.write(returnValue[1]+'\n')

def count(resultTable, table):
    num_rows = eval(table+".shape")
    count = num_rows[0]
    print(count)
    outputFile.write("Count+\n")
    outputFile.write(count+'\n')

def concat(resultTable, table, parameters):
    Q5 = np.concatenate((Q4, Q2), axis = 0)
    print (Q5)

def join(resultTable, table, parameters):
    table2 = parameters.split(',')[:1]
    newString = parameters.split(',')[1:]
    column1 = str(newString).split('=')[0].strip().strip('[]').strip("'")
    column1 = column1.strip()
    column2 = str(newString).split('=')[1].strip().strip('[]').strip("'")
    column1Table = column1.split('.')[0]
    finalColumn1 = column1.split('.')[1]
    column2Table = column2.split('.')[0]
    finalColumn2 = column2.split('.')[1]
    customerColumn = R['customerid']
    cColumn = S['C']
    table = np.where(R['customerid']==S['C'])
    rTable = R[table]
    sTable = S[table]
    print(rTable)
    print(sTable)
    outputFile.write("T1"+"\n")
    outputFile.write(rTable+'\n')
    outputFile.write(sTable+'\n')

def sort(resultTable, table, parameters):
    T1 = np.sort(T,axis = None, order = ['S_C'])
    outputFile.write("T1"+"\n")
    outputFile.write(T1+'\n')
    return T1
    

def sort1(resultTable, table, parameters):
    T2 = np.sort(T,axis = None, order = ['R1_time','S_C'])
    outputFile.write("T2"+"\n")
    outputFile.write(T2+'\n')
    return T2



def struct(argument):
    operation = re.search('^[^\(]*',argument).group(0)
    stringMatch = re.search('\(.+\)',argument).group(0)[1:-1]
    table = stringMatch.split(',')[0]
    parameters = stringMatch.strip(',')[3:].strip()
    return operation, table, parameters


        

def main():   
    timeStampFile = open("Time.txt","w")
    for line in sys.stdin:
            val = line.strip().split(':=')
            resultTable = val[0].strip()
            try:
                argument = val[1].strip()
            except IndexError:
                pass
            operation,table,parameters = struct(argument)

            if operation == 'inputfromfile':
                startTime = time.time()
                fileName = re.search('\(.+\)',argument).group(0)[1:-1]
                R,S = insert(fileName)
                endTime = time.time()
                inputTime = endTime - startTime
                timeStampFile.write("InputTime"+str(inputTime)+"\n")

            
            if operation == 'select':
                startTime = time.time()
                select(resultTable,table,parameters)
                endTime = time.time()
                selectTime = endTime - startTime
                timeStampFile.write("Selecttime"+str(selectTime)+"\n")

            if operation == 'project':
                startTime = time.time()
                project(resultTable,table,parameters)
                endTime = time.time()
                selectTime = endTime - startTime
                timeStampFile.write("R2.time"+str(selectTime)+"\n")
                #write project function

            if operation == 'avg':
                startTime = time.time()
                avg(resultTable,table,parameters)
                endTime = time.time()
                selectTime = endTime - startTime
                timeStampFile.write("R3.time"+str(selectTime)+"\n")
            
            if operation == 'sumgroup' and resultTable == 'R4':
                startTime = time.time()
                sumgroup(resultTable,table,parameters)
                endTime = time.time()
                selectTime = endTime - startTime
                timeStampFile.write("R4.time"+str(selectTime)+"\n")
            
            if operation == 'sumgroup' and resultTable == 'R5':
                startTime = time.time()
                sumgroup1(resultTable,table,parameters)
                endTime = time.time()
                selectTime = endTime - startTime
                timeStampFile.write("R5.time"+str(selectTime)+"\n")
            
            if operation == 'avggroup':
                startTime = time.time()
                avggroup(resultTable,table,parameters)
                endTime = time.time()
                selectTime = endTime - startTime
                timeStampFile.write("R6.time"+str(selectTime)+"\n")

            if operation == 'movsum':
                startTime = time.time()
                movsum(resultTable,table,parameters)
                endTime = time.time()
                selectTime = endTime - startTime
                timeStampFile.write("T4.time"+str(selectTime)+"\n")

            if operation == 'movavg':
                startTime = time.time()
                movavg(resultTable,table,parameters)
                endTime = time.time()
                selectTime = endTime - startTime
                timeStampFile.write("T3.time"+str(selectTime)+"\n")

            if 'Hash' in resultTable:
                global hashT 
                hashT = hashDB(resultTable)
                #write hash function
        
            if 'Btree' in resultTable:
                global btree1 
                btree1 = Btree(resultTable)

            if operation == 'count':
                startTime = time.time()
                count(resultTable,table)
                endTime = time.time()
                selectTime = endTime - startTime
                timeStampFile.write("count.time"+str(selectTime)+"\n")

            if operation == 'concat':
                startTime = time.time()
                concat(resultTable,table,parameters)
                endTime = time.time()
                selectTime = endTime - startTime
                timeStampFile.write("concat.time"+str(selectTime)+"\n")

            if operation == 'join':
                startTime = time.time()
                join(resultTable,table,parameters)
                endTime = time.time()
                selectTime = endTime - startTime
                timeStampFile.write("join.time"+str(selectTime)+"\n")
            
            if operation == 'sort':
                startTime = time.time()
                T1 = sort(resultTable,table,parameters)
                endTime = time.time()
                selectTime = endTime - startTime
                timeStampFile.write("T2.time"+str(selectTime)+"\n")

            
            if resultTable == 'T2Prime' and operation == 'sort':
                startTime = time.time()
                T2Prime = sort1(resultTable, table, parameters)
                endTime = time.time()
                selectTime = endTime - startTime
                timeStampFile.write("T3Prime.time"+str(selectTime)+"\n")
            
    
if __name__ == '__main__':
    main()
    
