import socket
import sys
import os

def client_program():
    host = socket.gethostname()  # as both code is running on same pc
    port = 6051  # socket server port number

    client_socket = socket.socket()  # instantiate
    client_socket.connect((host, port))  # connect to the server
    
    while True:
        try:
            data = client_socket.recv(1024).decode()
            if data:
                print("Received from Producer - ", data)
            client_socket.close()
        except KeyboardInterrupt:
            print("Interrupted")
            client_socket.close()
            sys.exit(0)

if __name__ == '__main__':
    client_program()