import os
from pyfiglet import figlet_format
from termcolor import cprint

cprint(figlet_format('KILL BROKERS'),
       'white', 'on_red', attrs=['bold'])
while True:
    que = input("Simulate Killing of Broker(b1,b2,b3)- ")
    if que == "b1":
        os.remove('brokers/b1.json')
    elif que == "b2":
        os.remove('brokers/b2.json')
    elif que == "b3":
        os.remove('brokers/b3.json')
    else:
        print("Enter Valid Broker")