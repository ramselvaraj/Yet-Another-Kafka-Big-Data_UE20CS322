import json
import os
import subprocess
import sys
from datetime import datetime
import pprint

currUser = "Guest"

def readJson(bname):
    with open('brokers/{}'.format(bname), 'r', encoding='utf-8') as f: 
        data = json.load(f)
    return data

def dumpToJson(bname,data):
    with open('brokers/{}'.format(bname), 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def getCurrLead():
    with open('leadBroker.txt','r',encoding='utf8') as f:
        leadBroker = f.readline()
    return leadBroker
def log(message):
    leadBroker = getCurrLead()
    data = readJson(leadBroker)
    data["log"].append(message)
    dumpToJson(leadBroker, data)

def subscribeTo(uname,topic):
    leadBroker = getCurrLead()
    data = readJson(leadBroker)
    if topic not in data['topics_Dict'].keys(): #checking if the key exists
        data['log'].append("{}".format(datetime.now()) + ' - Added New Topic [{}]'.format(topic)) #if the new topic does not exist we update it in the log 
        data['topics_Dict'][topic] = [] #we create a new dictionary for the new topic that consumer requested
        os.mkdir('topics\{}'.format(topic))
        with open('topics/{}/producers.json'.format(topic),'w', encoding='utf-8') as f:
            f.write('{}')
            data['consumers'][uname].append([topic,str(datetime.now())])
            log("{}".format(datetime.now()) + ' - Added New Topic [{}]'.format(topic))
    else: #the key exists
        for i in data['topics_Dict'][topic]: #going through the topic name
            with open('producers/{}'.format(i),'r',encoding='utf-8') as f: #opening the respective text files associated with the topic
                print(f.readlines()[1]) #displaying all the details of the topic
            data['consumers'][uname].append(i) #storing the consumer and topic they have subscribeTo
    data['log'].append("{}".format(datetime.now()) + " - {}".format(uname) + ' Subscribed to [{}]'.format(topic))  #updating the log
    dumpToJson(leadBroker, data)   #dumping the contents into json file
    log("{}".format(datetime.now()) + ' - Registered New Consumer [{}]'.format(uname))

def deregisterConsumer(uname):
    global currUser
    leadBroker = getCurrLead()
    data = readJson(leadBroker)
    data['consumers'].pop(uname)
    dumpToJson(leadBroker, data)
    currUser = "Guest"
    log("{}".format(datetime.now()) + ' - De-Registered Consumer [{}]'.format(uname))
    

def loginOrCreate(uname): #creating a login for the consumer so he can subscribe to the content
    leadBroker=getCurrLead()
    data=readJson(leadBroker)
    topicsDict = data['topics_Dict']
    if uname not in data['consumers'].keys(): #checking if consumer already exists or no
        data['consumers'][uname] = []
        dumpToJson(leadBroker, data)
        print("Choose from these topics, or create a new one".rjust(40, '-')) # the consumer either chooses the topic or creates a new one
        print(data['topics_Dict'].keys())
        topics = input('Enter your topics, seperated by comma - ') # the consmer enters the topic he wants to subscribeTo
        for i in topics.split(","):
            if(topicsDict.get(i)):
                pass
            else:
                topicsDict[i]=[]
                data['topics_Dict']=topicsDict
                os.mkdir('topics\{}'.format(i))
                with open('topics/{}/producers.json'.format(i),'w', encoding='utf-8') as f:
                    f.write('{}')
                log("{}".format(datetime.now()) + ' - Added New Topic [{}]'.format(uname))
            data['consumers'][uname].append([i,str(datetime.now())])#it will add the consumer and the topic he has subscribedTo 
        
    global currUser
    currUser = uname
    dumpToJson(leadBroker, data)
    log("{}".format(datetime.now()) + ' - Registered New Consumer [{}]'.format(uname))
    

def showTopics(uname): # a function to show topics
    with open('brokers/b1.json', 'r', encoding='utf-8') as f: 
         data = json.load(f)
    print("{}'s Topics-".format(uname))
    print(data['consumers'][uname])
message=""
message=input('->').lower()

def consume(uname):
    leadBroker=getCurrLead()
    data=readJson(leadBroker)
    
    ls = data['consumers'][uname]
    string = ""
    for i in ls:
        string = string + "|" + i[0]
    for i in data['consumers'][uname]:
        with open('topics\{}\producers.json'.format(i[0]),'r', encoding='utf-8') as f:
            data = json.load(f)
            for j in data.keys():
                if(datetime.strptime(data[j][:-7],'%Y-%m-%d %H:%M:%S') > datetime.strptime(i[1][:-7],'%Y-%m-%d %H:%M:%S')):
                    with open('producers/{}'.format(j),'r',encoding='utf-8') as f:
                        print(f.readlines()[1])
            log("{}".format(datetime.now()) + ' - [{}] viewed NEW messages from topics- '.format(uname) + string)

def consumeAll(uname):# defining the consumers
    leadBroker = getCurrLead()
    data = readJson(leadBroker)

    for i in data['consumers'][uname]:
        for j in data['topics_Dict'][i[0]]:
            with open('producers/{}'.format(j),'r',encoding='utf-8') as f:
                    print(f.readlines()[1])
    log("{}".format(datetime.now()) + ' - [{}] viewed ALL messages from topics- '.format(uname) + string)
def showLog(): # function for the logs
    leadBroker = getCurrLead()
    with open('brokers/{}'.format(leadBroker), 'r', encoding='utf-8') as f: 
         data = json.load(f)
    for i in data['log']:
        print(i)
# command line arguments for navigation
    log("{}".format(datetime.now()) + ' - [{}] viewed broker [{}] Log- '.format(currUser,leadBroker))

def showZooLog():
    with open('zooKeeperlog.txt', 'r', encoding='utf-8') as f: 
        lines = f.readlines()
    for i in lines:
        print(i)
# command line arguments for navigation
    log("{}".format(datetime.now()) + ' - [{}] viewed zookeeper Log- '.format(currUser))

while(message != 'quit'):
    
    if message == "print":
        print("hello world!")
    elif message == "change topic":
        newTopic = input("Enter New Topic Name- ")
    elif message == "subscribe to topilc":
        sub = input("Enter Topic Name- ")
        subscribeTo(currUser,sub)
    elif message == "login":
        uname = input("Enter Your User Name- ")
        loginOrCreate(uname)
    elif message == "deregister me":
        deregisterConsumer(currUser)
    elif message == "whoami":
        if currUser:
            print(currUser)
        else:
            print("Please Login")
    elif message == "show topics":
        showTopics(currUser)
    elif message == "consume --from-beginning":
        consumeAll(currUser)
    elif message == "consume":
        consume(currUser)
    elif message == "show log":
        showLog()
    elif message == "show zoo log":
        showZooLog()
    elif message == "logout":
        currUser = 'Guest'
    elif message == "quit":
        sys.exit(0)
    else:
        print("Please Enter Valid Command")
    message=(input('->')).lower()

