import json
import os
import subprocess
import sys
from datetime import datetime
import pprint

currUser = ""
def subscribeTo(uname,topic):
    data = []
    with open('brokers/b1.json', 'r', encoding='utf-8') as f:    #opening the b1.json file
        data = json.load(f) #loading the data file
    if topic not in data['topics_Dict'].keys(): #checking if the key exists
        data['log'].append("{}".format(datetime.now()) + ' - Added New Topic [{}]'.format(topic)) #if the new topic does not exist we update it in the log 
        data['topics_Dict'][topic] = [] #we create a new dictionary for the new topic that consumer requested
    else: #the key exists
        for i in data['topics_Dict'][topic]: #going through the topic name
            with open('producers/{}'.format(i),'r',encoding='utf-8') as f: #opening the respective text files associated with the topic
                print(f.readlines()[1]) #displaying all the details of the topic
            data['consumers'][uname].append(i) #storing the consumer and topic they have subscribeTo
    data['log'].append("{}".format(datetime.now()) + " - {}".format(uname) + ' Subscribed to [{}]'.format(topic))  #updating the log
    with open('brokers/b1.json', 'w',encoding='utf-8') as f: #rewriting the whole json file
            json.dump(data, f, ensure_ascii=False, indent=4)   #dumping the contents into json file

def loginOrCreate(uname): #creating a login for the consumer so he can subscribe to the content
    with open('brokers/b1.json', 'r', encoding='utf-8') as f: 
         data = json.load(f)
    if uname not in data['consumers'].keys(): #checking if consumer already exists or no
        data['consumers'][uname] = []
        with open('brokers/b1.json', 'w',encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
        #print (cstr.rjust(40, '-'))
        print("Choose from these topics, or create a new one".rjust(40, '-')) # the consumer either chooses the topic or creates a new one
        print(data['topics_Dict'].keys())
        topics = input('Enter your topics, seperated by comma - ') # the consmer enters the topic he wants to subscribeTo
        for i in topics.split(","):
            #subscribeTo(uname,i)
            data['consumers'][uname].append(i) #it will add the consumer and the topic he has subscribedTo 
    global currUser
    currUser = uname
    with open('brokers/b1.json', 'w',encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

def showTopics(uname): # a function to show topics
    with open('brokers/b1.json', 'r', encoding='utf-8') as f: 
         data = json.load(f)
    print("{}'s Topics-".format(uname))
    print(data['consumers'][uname])
message=""
message=input('->')

def consume(uname):# defining the consumers
    with open('brokers/b1.json', 'r', encoding='utf-8') as f: 
         data = json.load(f)
    
    for i in data['consumers'][uname]:
        for j in data['topics_Dict'][i]:
            with open('producers/{}'.format(j),'r',encoding='utf-8') as f:
                    print(f.readlines()[1])

def showLog(): # function for the logs
    with open('brokers/b1.json', 'r', encoding='utf-8') as f: 
         data = json.load(f)
    for i in data['log']:
        print(i)
# command line arguments for navigation
while(message != 'quit'):
    
    if message == "print":
        print("hello world!")
    elif message == "change topic":
        newTopic = input("Enter New Topic Name- ")
    elif message == "subscribe to topic":
        sub = input("Enter Topic Name- ")
        subscribeTo(currUser,sub)
    elif message == "Login":
        uname = input("Enter Your User Name- ")
        loginOrCreate(uname)
    elif message == "whoami":
        print(currUser)
    elif message == "show topics":
        showTopics(currUser)
    elif message == "consume":
        consume(currUser)
    elif message == "show log":
        showLog()
    message=input('->')
