import time
from os.path import exists
import shutil
import sys
from datetime import datetime
import json
import os
import subprocess
import threading
import concurrent.futures

def dumpToJson(bname,data): #function to dump to Json
    with open('brokers/{}'.format(bname), 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def readJson(bname):
    with open('brokers/{}'.format(bname), 'r', encoding='utf-8') as f: 
        data = json.load(f)
    return data

def getCurrLead():
    with open('leadBroker.txt','r',encoding='utf8') as f:
        leadBroker = f.readline()
    return leadBroker

def log(message):
    with open('zooKeeperlog.txt','a',encoding='utf8') as f:
        f.write("\n"+message)

#checking if the file still exist ( checking the health of the broker )
def heartbeatDisplay(broker):  
    if(exists('brokers/{}'.format(broker))):
        pass
    else:
        print("{} offline.".format(broker))
        #log("{}".format(datetime.now()) + ' - [{}] now offline '.format(broker))
def heartbeat(broker):
    if(exists('brokers/{}'.format(broker))):
        return True
    else:
        return False

        
#selecting new  leader if leader broker fails

def setLeader(leadBroker):
    with open('leadBroker.txt','w',encoding='utf8') as f:
        f.write(leadBroker)
        
def leaderSelection(leadBroker):
    
    if leadBroker == "b1.json":
        if(heartbeat("b1.json")):
            setLeader("b1.json")
            #log("{}".format(datetime.now()) + ' - [{}] is now Lead Broker '.format("b1.json"))
            return ["b1.json","b1.json"]
        else:
            setLeader("b2.json")
            log("{}".format(datetime.now()) + ' - [{}] is now Lead Broker '.format("b2.json"))
            return ["b2.json","b1.json"]
    elif leadBroker == "b2.json":
        if heartbeat("b1.json"):
            setLeader("b1.json")
            log("{}".format(datetime.now()) + ' - [{}] is now online and back as Lead Broker '.format("b1.json"))
            return ["b1.json","b2.json"]
        elif(heartbeat("b2.json")):
            setLeader("b2.json")
            return ["b2.json","b2.json"]
        else:
            setLeader('b3.json')
            log("{}".format(datetime.now()) + ' - b2.json is now offline and [{}] is now Lead Broker '.format("b3.json"))
            return ["b3.json","b2.json"]
    else:
        if heartbeat("b1.json"):
            setLeader('b1.json')
            log("{}".format(datetime.now()) + ' - [{}] is now online and back as Lead Broker '.format("b1.json"))
            return ["b1.json","b3.json"]
        elif heartbeat("b2.json"):
            setLeader('b2.json')
            log("{}".format(datetime.now()) + ' - [{}] is now online and back as Lead Broker '.format("b2.json"))
            return ["b2.json","b3.json"]
        
