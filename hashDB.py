import re
import time
datafile = "C:\\Users\\admin\\Desktop\\dataFull.txt" #data file path
f = open(datafile, "r")

searchFile = open('SearchFile.txt','w')  #Output Search file

hashTable = {}
dataTable = []
index = 0
for line in f:
    key, value = line.strip().split('|')
    dataTable.append([key,value])
    index += 1
    hashTable[key] = index

inputFile = "C:\\Users\\admin\\Desktop\\inputFile.txt" #input file path
commandFile = open(inputFile,'r')
startTime = time.time()

try:
    def search(hkey):
        searchIndex = hashTable[hkey]
        returnValue = dataTable[searchIndex]
        return returnValue[1]

    def delete(hkey):
        deleteIndex = hashTable[hkey]
        dataTable[deleteIndex] = [hkey,'deleted']
        del hashTable[hkey]

    def insert(hkey,hvalue):
       dataTable.append([hkey,hvalue])
       global index
       index += 1
       hashTable[key] = index


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
            if key in hashTable:
                searchReturn = search(key)
                searchFile.write(searchReturn + "\n")
            else:
                print("Key not present\n")
        if line[0].lower() == 'd':
            stringMatch = re.search('\(.+\)',line)
            stringMatch = stringMatch.group(0)
            key = stringMatch[1:-1]
            if key in hashTable:
                delete(key)
            else:
               print("Key not present\n") 


except IndexError:
    print("Please provide two values for Insert\n")

endTime = time.time()
timeTaken = endTime - startTime
printTable = open("FinalTable.txt",'w')
for val in dataTable:
    val = str(val)+"\n"
    printTable.write(val)
timeStampFile = open("TimeTaken.txt","w")
timeStampFile.write(str(timeTaken))

#print(hashTable)
#print(dataTable)