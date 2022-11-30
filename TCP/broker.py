import socket
import threading

import socket
import sys
import os

dataGlob = ""
dataGlobBuff = ""
def producer_to_broker():
    host = socket.gethostname()  # as both code is running on same pc
    port = 6050  # socket server port number
 #initiate port no above 1024

    client_socket = socket.socket()  # instantiate
    client_socket.connect((host, port))  # connect to the server

    while True:
        try:
            global dataGlob
            global dataGlobBuff
            #if dataGlob:
            data = client_socket.recv(1024).decode()
            dataGlobBuff = data
            dataGlob = data
        except KeyboardInterrupt:
            print("Interrupted")
            client_socket.close()
            sys.exit()

def broker_to_consumer():
    host = socket.gethostname()
    port = 6051
    server_socket = socket.socket()
    server_socket.bind((host, port))
    server_socket.listen(2)
    conn, address = server_socket.accept() 
    
    while True:
        try:
            global dataGlobBuff
            if dataGlobBuff:
                conn.send(dataGlobBuff.encode())
        except KeyboardInterrupt:
            print("Interrupted")
            conn.close()
            sys.exit(0)
            
            
t1 = threading.Thread(target=producer_to_broker)
t2 = threading.Thread(target=broker_to_consumer)
t1.start()
t2.start()