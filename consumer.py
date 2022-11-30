import json
import os
import subprocess
import sys
from datetime import datetime
import pprint

currUser = ""
def subscribeTo(uname,topic):
    data = []
    with open('brokers/b1.json', 'r', encoding='utf-8') as f:    
        data = json.load(f)
    if topic not in data['topics_Dict'].keys():
        data['log'].append("{}".format(datetime.now()) + ' - Added New Topic [{}]'.format(topic))
        data['topics_Dict'][topic] = []
    #else:
        #for i in data['topics_Dict'][topic]:
            # with open('producers/{}'.format(i),'r',encoding='utf-8') as f:
            #     print(f.readlines()[1])
    data['consumers'][uname].append(i)
    data['log'].append("{}".format(datetime.now()) + " - {}".format(uname) + ' Subscribed to [{}]'.format(topic))  
    with open('brokers/b1.json', 'w',encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)   

def loginOrCreate(uname):
    with open('brokers/b1.json', 'r', encoding='utf-8') as f: 
         data = json.load(f)
    if uname not in data['consumers'].keys():
        data['consumers'][uname] = []
        with open('brokers/b1.json', 'w',encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
        #print (cstr.rjust(40, '-'))
        print("Choose from these topics, or create a new one".rjust(40, '-'))
        print(data['topics_Dict'].keys())
        topics = input('Enter your topics, seperated by comma - ')
        for i in topics.split(","):
            #subscribeTo(uname,i)
            data['consumers'][uname].append(i)
    global currUser
    currUser = uname
    with open('brokers/b1.json', 'w',encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

def showTopics(uname):
    with open('brokers/b1.json', 'r', encoding='utf-8') as f: 
         data = json.load(f)
    print("{}'s Topics-".format(uname))
    print(data['consumers'][uname])
message=""
message=input('->')

def consume(uname):
    with open('brokers/b1.json', 'r', encoding='utf-8') as f: 
         data = json.load(f)
    
    for i in data['consumers'][uname]:
        for j in data['topics_Dict'][i]:
            with open('producers/{}'.format(j),'r',encoding='utf-8') as f:
                    print(f.readlines()[1])

def showLog():
    with open('brokers/b1.json', 'r', encoding='utf-8') as f: 
         data = json.load(f)
    for i in data['log']:
        print(i)

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
