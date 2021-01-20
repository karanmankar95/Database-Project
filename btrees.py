import re
from BTrees.OOBTree import OOBTreePy
import timeit
datafile = "D:\\DB HW\\dataFull.txt" #data file path
f = open(datafile, "r")

searchFile = open('SearchFileBtree.txt','w')  #Output Search file

btree = OOBTreePy()
dataTable = []
index = 0
for line in f:
    key, value = line.strip().split('|')
    dataTable.append([key,value])
    index += 1
    btree[key] = index

inputFile = "D:\\DB HW\\inputFile.txt" #input file path
commandFile = open(inputFile,'r')
startTime = timeit.timeit()

try:
    def search(hkey):
        searchIndex = btree[hkey]
        returnValue = dataTable[searchIndex]
        return returnValue[1]

    def delete(hkey):
        deleteIndex = btree[hkey]
        dataTable[deleteIndex] = [hkey,'deleted']
        del btree[hkey]

    def insert(hkey,hvalue):
       dataTable.append([hkey,hvalue])
       global index
       index += 1
       btree[key] = index


    for line in commandFile:
        if line[0].lower() == 'i':
            stringMatch = re.search('\(.+\)',line)
            stringMatch = stringMatch.group(0)
            stringMatch = stringMatch[1:-1].split(',')
            key = stringMatch[0]
            value = stringMatch[1]
            insert(key,value)
        if line[0].lower() == 's':
            stringMatch = re.search('\(.+\)',line)
            stringMatch = stringMatch.group(0)
            key = stringMatch[1:-1]
            if key in btree:
                searchReturn = search(key)
                searchFile.write(searchReturn + "\n")
            else:
                print("Key not present\n")
        if line[0].lower() == 'd':
            stringMatch = re.search('\(.+\)',line)
            stringMatch = stringMatch.group(0)
            key = stringMatch[1:-1]
            if key in btree:
                delete(key)
            else:
               print("Key not present\n") 


except IndexError:
    print("Please provide two values for Insert\n")

endTime = timeit.timeit()
timeTaken = endTime - startTime
printTable = open("FinalTableBtree.txt",'w')
for val in dataTable:
    val = str(val)+"\n"
    printTable.write(val)
timeStampFile = open("TimeTakenBtree.txt","w")
timeStampFile.write(str(startTime)+"\n")
timeStampFile.write(str(endTime)+"\n")
timeStampFile.write(str(timeTaken))

