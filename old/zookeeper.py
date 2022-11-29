import pika
import os
import sys

from kafka import myBroker

def checkStatus():
    pass
def showBrokers():
    pass
def viewTopics():
    pass

def main():
    broker1 = myBroker('Brokie1')
    broker2 = myBroker('Brokie2')
    broker3 = myBroker('Brokie3')

    message = input("->")
    while message != "quit":
        match message:
            case "check status":
                checkStatus()
            case "show brokers":
                showBrokers()
            case "view topics":
                viewTopics()
            case default:
                print("Please Enter Valid Command")
        message = input("->")

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)


