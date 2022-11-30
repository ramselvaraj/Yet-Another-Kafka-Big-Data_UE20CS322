import json
import os
import subprocess
import sys
import shutil
import threading
from datetime import datetime
from zookeeper import heartbeat
from zookeeper import leaderSelection
import concurrent.futures

leadBroker="b1.json"
def replicateb2():
    global leadBroker
    if leadBroker == "b1.json":
        shutil.copyfile('brokers/b1.json','brokers/b2.json')
        shutil.copyfile('brokers/b1.json','brokers/b3.json')
    elif leadBroker == "b2.json":
        shutil.copyfile('brokers/b2.json','brokers/b1.json')
        shutil.copyfile('brokers/b2.json','brokers/b3.json')
    else:
        shutil.copyfile('brokers/b3.json','brokers/b1.json')
        shutil.copyfile('brokers/b3.json','brokers/b2.json')
#def replicateb3():
    #shutil.copyfile('brokers/b1.json','brokers/b3.json')
    #shutil.copyfile('brokers/b1.json','brokers/b2.json')
    #shutil.copyfile('brokers/b1.json','brokers/b3.json')
f = open('brokers/b1.json')
data = json.load(f)
#data['no_of_producers'] = int(sys.argv[1])
topicsDict = data['topics_Dict']
f.close()
producers=os.listdir((os.path.dirname("producers/")))
data['no_of_producers'] = len(producers)
print(producers)

for i in producers:
    with open('producers/{}'.format(i),'r',encoding='utf8') as f:
        topic=f.readline()
        topic=topic.strip()
        if(topicsDict.get(topic)):
            if i not in topicsDict[topic]:
                topicsDict[topic].append(i)
        else:
            data['log'].append("{}".format(datetime.now()) + ' - Added New Topic [{}]'.format(topic))
            #data['log'].append("Hello")
            topicsDict[topic]=[i]
    data['topics_Dict']=topicsDict
with open('brokers/b1.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=4)

topicsDict = {}
# with open('brokers/b1.json', 'r', encoding='utf-8') as f:
#     data = json.load(f)
#     data['no_of_producers'] = int(sys.argv[1])
#     topicsDict = data['topics_Dict']

# f = open('brokers/b1.json')
# data = json.load(f)
# data['no_of_producers'] = int(sys.argv[1])
# topicsDict = data['topics_Dict']
# f.close()

def main():
    global leadBroker
    #leadBroker = "b2.json" 
    # t3 = threading.Thread(target=heartbeat,args=('b1.json',))
    # t4 = threading.Thread(target=heartbeat,args=('b2.json',))
    # t5 = threading.Thread(target=heartbeat,args=('b3.json',))
    
    # t3.start()
    # t4.start()
    # t5.start()  
    print(leadBroker)
    while True:
        
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(leaderSelection, leadBroker)
            leadBroker = future.result()
        t1 = threading.Thread(target=replicateb2)
        #t2 = threading.Thread(target=replicateb3)
        #t6 = threading.Thread(target=leaderSelection,args=(leadBroker,))
        #t6 = threading.Thread(target=scanProducers)
        t1.start()
        #t2.start()
        #t6.start()
        #leadBroker=t6.join()
        t3 = threading.Thread(target=heartbeat,args=('b1.json',))
        t4 = threading.Thread(target=heartbeat,args=('b2.json',))
        t5 = threading.Thread(target=heartbeat,args=('b3.json',))
        t3.start()
        t4.start()
        t5.start()
    
        

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Keyboard Interrupt')
        sys.exit(0)
        