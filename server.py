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
        #Accepting incoming connections
        received_data = conn.recv(1024)
        #Deserialize the string to array
        data = pickle.loads(received_data)
        print "[*] Received '",data,"' from the client"
        print "        Processing data"

        if data=="disconnect":
            conn.send("Goodbye")
            conn.close()
            break;

        global total_partition
        total_partition = data.pop()
        method_client_called = data.pop()

        if method_client_called =="create":
            for element in data:
                 create_topic(element)

            topic_partition = {"topic_name":data[0][0], "partition_num":total_partition, "clients_name":[]}
            topic_partition_assignment.append(topic_partition)
            reply_from_server = "Successfully created "+total_partition+" partitions."
            conn.send(reply_from_server)
            print "    Processing done, data was valid.\n[*] Reply sent"

        elif method_client_called =="subscribe":
            conn.send(subscribe(data[0],data[1]))

        else:
            conn.send("Invalid Request")
            print "    Processing done.\n[*] Reply sent"


def create_topic(topic_info_array):
    data_struct.append(topic_info_array)

def subscribe(topic_name, user_name):
    for topic in topic_partition_assignment:
        if topic_name == topic["topic_name"] and topic["clients_name"]==[]:
            topic["clients_name"].append(user_name)
            all_partitions = list(range(int(total_partition)))

            global partition_server1
            partition_server1 = []
            global partition_server2
            partition_server2 = []
            for partition in all_partitions:
                if partition%2 == 0:
                    partition_server1.append(partition)
                elif partition%2 == 1:
                    partition_server2.append(partition)
            partition_server1 = map(str, partition_server1)
            partitions_1 = ','.join(partition_server1)
            reply_from_server_1= user_name+' subscribed to '+ topic_name+ ' and can get partition '+ partitions_1+ " from server1."
            if partition_server2 == []:
                return reply_from_server_1
            else:
                partition_server2 = map(str, partition_server2)
                partitions_2 = ','.join(partition_server2)
                reply_from_server_2 = "and get partition " + partitions_2 + " from server2."
                reply_from_server = reply_from_server_1 + reply_from_server_2
                return reply_from_server_1 + reply_from_server_2

        elif topic_name == topic["topic_name"] and topic["clients_name"]!=[]:
            if len(topic["clients_name"]) == int(topic["partition_num"]):
                reply_from_server = "No more partition left!"
                return reply_from_server
            elif:
                if user_name in topic["clients_name"]:
                    return "You have already subscribed for this topic!"
                # else:
                #     topic["clients_name"].append(user_name)
                #     all_partitions = list(range(int(total_partition)))
                #     for partition in all_partitions:
                #         if partition / len(topic["clients_name"]) == 0:
                #             partition_server1.append(partition)
                #         elif partition% == 1:
                #             partition_server2.append(partition)
                #     partition_server1 = map(str, partition_server1)
                #     partitions_1 = ','.join(partition_server1)
                #     reply_from_server_1= user_name+' subscribed to '+ topic_name+ ' and can get partition '+ partitions_1+ " from server1."
                #     if partition_server2 == []:
                #         return reply_from_server_1
                #     else:
                #         partition_server2 = map(str, partition_server2)
                #         partitions_2 = ','.join(partition_server2)
                #         reply_from_server_2 = "and get partition " + partitions_2 + " from server2."
                #         reply_from_server = reply_from_server_1 + reply_from_server_2
                #         return reply_from_server_1 + reply_from_server_2


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
    while True:
        conn, addr = server.accept()
        print "[*] Got a connection from ", addr[0],":",addr[1]
        #Creating new thread. Calling clientthread function for this function and passing conn as argument.
        start_new_thread(clientthread,(conn,)) #start new thread takes 1st argument as a function name to be run, second is the tuple of arguments to the function.

    conn.close()
    server.close()

if __name__ == '__main__':

    Main(sys.argv[1:])
