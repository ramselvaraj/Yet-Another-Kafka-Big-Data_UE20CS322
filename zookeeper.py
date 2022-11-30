import time
from os.path import exists
import shutil
import sys
#checking if the file still exist ( checking the health of the broker )
def heartbeat(broker):  
    # while True:
    #     try:
    #         if(exists('brokers/{}'.format(broker))):
    #             pass
    #         else:
    #             print("{} offline.".format(broker))
    #             #leaderSelection()
    #         time.sleep(5)
    #     except KeyboardInterrupt:
    #         print('Keyboard Interrupt',broker)
    #         sys.exit(0)
    if(exists('brokers/{}'.format(broker))):
        pass
    else:
        print("{} offline.".format(broker))
                #leaderSelection()
    time.sleep(5)

#selecting new  leader if leader broker fails
def leaderSelection(leadBroker):
    
    if leadBroker == "b1.json":
        if(exists('brokers/b1.json')):
            return "b1.json"
        else:
            return "b2.json"
        #print(leadBroker)
        #shutil.copyfile('brokers/b1.json','brokers/b2.json')#copying files from b1.json to b2.json
        #shutil.copyfile('brokers/b1.json','brokers/b3.json')#copying flies from b1.json to b3.json
    elif leadBroker == "b2.json":
        #print(leadBroker)
        if exists('brokers/b1.json'):
            leadBroker = "b1.json"
            return "b1.json"
        elif(exists('brokers/b2.json')):
            return "b2.json"
        else:
            return "b3.json"
        #shutil.copyfile('brokers/b2.json','brokers/b1.json')
        #shutil.copyfile('brokers/b2.json','brokers/b3.json')
    else:
        if exists('brokers/b1.json'):
            leadBroker = "b1.json"
        elif exists('brokers/b2.json'):
            leadBroker = "b2.json"
        #shutil.copyfile('brokers/b3.json','brokers/b1.json')
        #shutil.copyfile('brokers/b3.json','brokers/b2.json')
        
