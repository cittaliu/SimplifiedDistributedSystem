import socket
import argparse
from _thread import *
import sys


def clientthread(conn):
    #infinite loop so that function do not terminate and thread do not end.
    while True:
        #Sending message to connected client
        #Receiving from client
        data = conn.recv(1024) # 1024 stands for bytes of data to be received
        print(data)


data_struct = []

def create_topic(topic_name, partition_number, key="", value=0):
    data_struct.append( [topic_name,partition_number,key,value] )

def subscribe(topic_name):
    for topic in data_struct:
        if topic_name == topic[0]:
            print('subscribed to', topic_name, 'and can get partition' , topic[1])
            return topic[1]

def publish_topic(topic_name,key,value):
    for topic in data_struct:
        if topic_name == topic[0]:
            topic[2] = key
            topic[3] = value
            print ('put ','("',key,'")', value , 'on partition ', topic[1])

def get_topic(topic_name):
    for topic in data_struct:
        if topic_name == topic[0]:
            print('get','("',topic[2],'"),',topic[3],'from topic',topic_name,'and',topic[1])




def main(argv):
    name = socket.gethostname()
    host = socket.gethostbyname(name)
    server = socket.socket()
    server.bind(('', 0))
    server.listen(5)
    port = server.getsockname()[1]
    server_name = ['']

    # parses argument
    arglist = sys.argv
    if len(arglist) == 1:
        print ('No servename given\n')
        sys.exit()
    if len(arglist) == 2:
        server_name = arglist[1]

    if len(arglist) >= 3:
        print('No servename given\n')
        sys.exit()

    print ("[*]",server_name,"started listening on ",host,":",port)


    while True:
        #Accepting incoming connections
        conn, addr = server.accept()
        print ("[*] Got a connection from ", addr[0],":",addr[1])
        #Creating new thread. Calling clientthread function for this function and passing conn as argument.
        start_new_thread(clientthread,(conn,)) #start new thread takes 1st argument as a function name to be run, second is the tuple of arguments to the function.

    conn.close()
    server.close()



## here i am getting the parameters for the command line


if __name__ == "__main__":
   main(sys.argv[1:])