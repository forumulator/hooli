import os
import re

#Creates a Directory with its name as Project Name.
def makeDir(projectName):
    print('Creating Directory '+ projectName)
    os.makedirs(projectName)
    print(projectName + ' created')
    os.makedirs('HTML')
    os.makedirs('PDFs')

#Creates various files for managing links.
def createDataFiles(projectName,baseURL):
    listFile = projectName + '/list.txt'
    brokenFile = projectName + '/broken.txt'
    problemFile = projectName + '/problem.txt'
    linkFile = projectName + '/link.txt'
    unknownFile = projectName + '/unknown.txt'
    graphFile = projectName + '/graph.txt'
    skipFile = projectName + '/skip.txt'


    writeFile(listFile,baseURL)
    writeFile(brokenFile,'')
    writeFile(problemFile,'')
    writeFile(linkFile,'')
    writeFile(unknownFile, '')
    writeFile(graphFile, '')
    writeFile(skipFile,'')


#Creates new file
def writeFile(path,data):
    f = open(path, 'w', encoding='utf-8')
    f.write(data + '\n')
    f.close()

#Add to file
def appendToFile(path,data):
    f = open(path,'a', encoding='utf-8')
    f.write(data + '\n')
    f.close()

#Edit string to normalize it a little
def cleanLink(url):
    asd = url.replace('http://','')
    asd = asd.replace('https://','')
    my_new_string = re.sub('[^a-zA-Z0-9 \.]', '_', asd)
    return my_new_string


#Normalize URLs better
def urlFix(url):
    asd = url.replace(' ', '%20')
    asd = asd.replace('"','%22')
    asd = asd.replace('(', '%28')
    asd = asd.replace(')', '%29')
    asd = asd.replace("'", '%27')

#Splits the url using the # if present
    head, sep, tail = asd.partition('#')

    return head