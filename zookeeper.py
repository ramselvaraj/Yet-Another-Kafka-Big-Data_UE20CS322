import time
from os.path import exists

def heartbeat(broker):
    while True:
        if(exists('brokers/{}'.format(broker))):
            pass
        else:
            print("{} offline.".format(broker))
            #leaderSelection()
        time.sleep(5)

def leaderSelection():
    
    if leadBroker == "b1.json":
        # if (exists('brokers/b1.json'):
        #     pass
        # else:

        print(leaderBroker)
        shutil.copyfile('brokers/b1.json','brokers/b2.json')
        shutil.copyfile('brokers/b1.json','brokers/b3.json')
    elif leadBroker == "b2.json":
        print(leaderBroker)
        if exists('brokers/b1.json'):
            leadBroker = "b1.json"
        shutil.copyfile('brokers/b2.json','brokers/b1.json')
        shutil.copyfile('brokers/b2.json','brokers/b3.json')
    else:
        if exists('brokers/b1.json'):
            leadBroker = "b1.json"
        elif exists('brokers/b2.json'):
            leadBroker = "b2.json"
        shutil.copyfile('brokers/b3.json','brokers/b1.json')
        shutil.copyfile('brokers/b3.json','brokers/b2.json')
        
