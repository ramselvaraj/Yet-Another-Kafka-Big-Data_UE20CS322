import socket
import os
import sys

def producer_to_broker():
    # get the hostname
    host = socket.gethostname()
    port = 6050  # initiate port no above 1024

    server_socket = socket.socket()  # get instance
    # look closely. The bind() function takes tuple as argument
    server_socket.bind((host, port))  # bind host address and port together

    # configure how many client the server can listen simultaneously
    server_socket.listen(2)
    conn, address = server_socket.accept()  # accept new connection
    print("Connection from: " + str(address))
    
    while True:
        try:
            data = input("Enter a Producer Message: ")
            conn.send(data.encode())
        except KeyboardInterrupt:
            print("Interrupted")
            conn.close()
            sys.exit(0)



if __name__ == '__main__':
    producer_to_broker()