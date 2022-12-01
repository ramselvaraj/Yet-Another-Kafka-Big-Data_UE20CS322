import json
import os
import subprocess
import sys
import shutil
import threading
from datetime import datetime
from zookeeper import heartbeatDisplay
from zookeeper import heartbeat
from zookeeper import leaderSelection
import concurrent.futures
import time

prevLeaderBroker = "b1.json" #has the value of the previous broker
leadBroker="b1.json" #has the value of the current broker
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
    leadBroker = getCurrLead()
    data = readJson(leadBroker)
    data["log"].append(message)
    dumpToJson(leadBroker, data)

def replicateb2(): #handles replication of the lead broker across the other brokers 
    brokerlist=["b1.json", "b2.json", "b3.json"]
    global leadBroker
    global prevLeaderBroker
    if os.stat("brokers/{}".format(leadBroker)).st_size!=0 :
        ls=[x for x in brokerlist if x != leadBroker]
        shutil.copyfile('brokers/{}'.format(leadBroker),'brokers/{}'.format(ls[0]))
        shutil.copyfile('brokers/{}'.format(leadBroker),'brokers/{}'.format(ls[1]))
    else:
        shutil.copyfile('brokers/{}'.format(prevLeaderBroker),'brokers/{}'.format(leadBroker))

    
def registerProducer(topic,pname):
    leadBroker = "b1.json"
    with open('leadBroker.txt','r',encoding='utf8') as f:
        leadBroker = f.readline()
    data=readJson(leadBroker)
    topicsDict = data['topics_Dict']
    with open('producers/{}'.format(pname),'r',encoding='utf8') as f:
        topic=topic.strip()
        if(topicsDict.get(topic)):
            if pname not in topicsDict[topic]:
                topicsDict[topic].append(pname)
                #ata['log'].append("{}".format(datetime.now()) + ' - Added New Producer [{}]'.format(pname))
                log("{}".format(datetime.now()) + ' - Registered New Producer [{}]'.format(pname))
        else:
            data["log"].append("{}".format(datetime.now()) + ' - Added New Topic [{}]'.format(topic))
            topicsDict[topic]=[pname] 
            log("{}".format(datetime.now()) + ' - Registered New Producer [{}]'.format(pname))   
        data['topics_Dict']=topicsDict
    dumpToJson(leadBroker, data)
    if(os.path.exists('topics/{}'.format(topic))==False):
        os.mkdir('topics/{}'.format(topic))
        with open('topics/{}/producers.json'.format(topic),'w', encoding='utf-8') as f:
            f.write('{}')
    with open('topics/{}/producers.json'.format(topic),'r',encoding='utf-8') as f:
        data = json.load(f)  
    data[pname]=str(datetime.now())     
    with open('topics/{}/producers.json'.format(topic),'w',encoding='utf-8') as f:
        json.dump(data,f,ensure_ascii=False, indent=4)


def deregisterProducer(topic,pname):
    leadBroker = getCurrLead()
    data=readJson(leadBroker)
    #topicsDict = data['topics_Dict']
    data['topics_Dict'][topic] = [x for x in data['topics_Dict'][topic] if x != pname]
    dumpToJson(leadBroker, data)
    with open('topics/{}/producers.json'.format(topic),'r',encoding='utf-8') as f:
        data = json.load(f)  
    data.pop(pname)   
    with open('topics/{}/producers.json'.format(topic),'w',encoding='utf-8') as f:
        json.dump(data,f,ensure_ascii=False, indent=4)
    log("{}".format(datetime.now()) + ' - De Registered Producer [{}]'.format(pname))

topicsDict = {}

def main():
    global leadBroker
    global prevLeaderBroker
    print("Current Leader:{}".format(leadBroker))
    while True:
        time.sleep(2)
        print("Leader:{}".format(leadBroker))
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(leaderSelection, leadBroker)
            leadBroker = future.result()[0]
            prevLeaderBroker=future.result()[1]
        t1 = threading.Thread(target=replicateb2)
        t1.start()
        t3 = threading.Thread(target=heartbeatDisplay,args=('b1.json',))
        t4 = threading.Thread(target=heartbeatDisplay,args=('b2.json',))
        t5 = threading.Thread(target=heartbeatDisplay,args=('b3.json',))
        t3.start()
        t4.start()
        t5.start()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Keyboard Interrupt')
        sys.exit(0)
        
