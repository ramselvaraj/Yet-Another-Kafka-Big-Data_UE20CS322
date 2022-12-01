import json
import os
import subprocess
import sys
from datetime import datetime
import pprint
from broker import registerProducer
from broker import deregisterProducer
from yahoo_finance import Share

TopicName=sys.argv[1]
fileName=sys.argv[2]

if fileName == "stock":
    print("Enter Hist Date Range")
    date1 = input('Enter Start Date (YYYY-MM-DD)- ')
    #date1 = datetime.strptime(date_string, '%Y-%m-%d')
    date2 = input('Enter End Date (YYYY-MM-DD)- ')
    #date2 = datetime.strptime(date_string, '%Y-%m-%d')
    info = Share(TopicName)
    stockDict = info.get_historical(date1, date2)
    stockDict = json.load(stockDict)
    producers=os.listdir((os.path.dirname("producers/")))
    num=int(len(producers)) + 1
    print(num)
    f=open("producers/p{}.txt".format(num), 'w')
    f.write(TopicName+"\n")
    f.write(stockDict)
    registerProducer(TopicName,"p{}.txt".format(num))
    
if TopicName == "-deregister":
    f = open('producers/{}'.format(fileName), 'r')
    topic = f.readline().strip()
    f.close()
    deregisterProducer(topic,fileName)
    os.remove("producers/{}".format(fileName))

else:
    f = open('{}'.format(fileName), 'r')
    lines = f.readlines()
    mystr = '\n'.join([line.strip() for line in lines])
    f.close()
    string=""
    for i in lines:
        string+=i.strip()
    producers=os.listdir((os.path.dirname("producers/")))
    num=int(len(producers)) + 1
    print("Registered Producer p{}.txt".format(num))
    f=open("producers/p{}.txt".format(num), 'w')
    f.write(TopicName+"\n")
    f.write(string)
    registerProducer(TopicName,"p{}.txt".format(num))



