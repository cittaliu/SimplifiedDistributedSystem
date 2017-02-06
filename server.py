import socket
from thread import *
import sys
import pickle

# Database to store all the topics' info
data_struct = []

topic_partition_assignment = []


def clientthread(conn):
    #infinite loop so that function do not terminate and thread do not end.
    while True:
        #Sending message to connected client
        #Receiving from client
        data = conn.recv(1024) # 1024 stands for bytes of data to be received
        print data




def create_topic(topic_info_array):
    data_struct.append(topic_info_array)


def subscribe(topic_name, user_name):
    for topic in data_struct:
        count = 0
        if topic_name == topic[0] and topic[4]=='':
            topic[4] = user_name

            print 'subscribed to', topic_name, 'and can get partition' , topic[1], "from", server_name
        # elif topic_name == topic[0] and topic[4]!='':








def publish_topic(topic_name,key,value):
    for topic in data_struct:
        if topic_name == topic[0]:
            topic[2] = key
            topic[3] = value
            print 'put ','("',key,'")', value , 'on partition ', topic[1]

def get_topic(topic_name):
    for topic in data_struct:
        if topic_name == topic[0]:
            print 'get','("',topic[2],'"),',topic[3],'from topic',topic_name,'and',topic[1]


def Main(argv):
    name = socket.gethostname()
    host = socket.gethostbyname(name)
    server = socket.socket()
    server.bind(('', 0))
    server.listen(5)
    port = server.getsockname()[1]
    global server_name
    server_name = ['']

    arglist = sys.argv
    if len(arglist) == 1:
        print 'No servename given\n'
        sys.exit()
    if len(arglist) == 2:
        server_name = arglist[1]

    if len(arglist) >= 3:
        print 'No servename given\n'
        sys.exit()

    print "[*]",server_name,"started listening on ",host,":",port

    conn, addr = server.accept()
    print "[*] Got a connection from ", addr[0],":",addr[1]
    #Creating new thread. Calling clientthread function for this function and passing conn as argument.
    start_new_thread(clientthread,(conn,)) #start new thread takes 1st argument as a function name to be run, second is the tuple of arguments to the function.


    while True:
        #Accepting incoming connections
    	received_data = conn.recv(1024)
        #Deserialize the string to array
        data = pickle.loads(received_data)
        print "[*] Received '",data,"' from the client"
        print "		Processing data"

        if data=="disconnect":
            conn.send("Goodbye")
            conn.close()
            break;

        global total_partition
        total_partition = data.pop()
        method_client_called = data.pop()
        print "Successfully created",total_partition,"partitions."

        if method_client_called =="create":
            for element in data:
    	         create_topic(element)
            print data_struct

            topic_partition = {"topic":data[0][0], "partition_num":total_partition, "client":[]}
            topic_partition_assignment.append(topic_partition)
            print topic_partition

            print "	Processing done, data was valid.\n[*] Reply sent"
        elif method_client_called =="subscribe":
            for element in data:
    	         subscribe(element[0],element[1])

    	else:
    	    conn.send("Invalid Request")
    	    print "	Processing done.\n[*] Reply sent"

    conn.close()
    server.close()

if __name__ == '__main__':

    Main(sys.argv[1:])
