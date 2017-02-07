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


        method_client_called = data.pop()

        if method_client_called =="create":
            for element in data:
                 create_topic(element)

            total_partition = data.pop()
            topic_partition = {"topic_name":data[0][0], "partition_num":total_partition, "clients_name":[]}
            topic_partition_assignment.append(topic_partition)
            reply_from_server = "Successfully created "+total_partition+" partitions."
            conn.send(reply_from_server)
            print "    Processing done, data was valid.\n[*] Reply sent"

        elif method_client_called =="subscribe":
            conn.send(subscribe(data[0],data[1]))
            print "    Processing done, data was valid.\n[*] Reply sent"
        elif method_client_called =="publish":
            data_send_back = publish_topic(data[0],data[1][0],data[1][1],data[2])
            if data_send_back == '':
                data_send_back = "No matching record found in " + server_name
                conn.send(data_send_back)
            else:
                conn.send(data_send_back)
            print "    Processing done, data was valid.\n[*] Reply sent"
        elif method_client_called =="get":
            conn.send(get_topic(data[0],data[1]))
            print "    Processing done, data was valid.\n[*] Reply sent"
        else:
            conn.send("Invalid Request")
            print "    Processing done.\n[*] Reply sent"


def create_topic(topic_info_array):
    data_struct.append(topic_info_array)

def subscribe(topic_name, user_name):
    for topic in topic_partition_assignment:
        if topic_name == topic["topic_name"] and topic["clients_name"]==[]:
            topic["clients_name"].append(user_name)
            partitions = ''
            for topic_partition in topic_partition_assignment:
                if topic_partition["topic_name"] == topic_name:
                    partitions = topic_partition["partition_num"]
            all_partitions = list(range(int(partitions)))
            # The first user gets all partitions ~
            global first_user_partitions
            first_user_partitions = all_partitions
            global first_user_name
            first_user_name = user_name
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
            else:
                if user_name in topic["clients_name"]:
                    return "You have already subscribed to this topic!"
                else:
                    new_user_partition = first_user_partitions.pop()
                    first_user_partitions = map(str, first_user_partitions)
                    first_user_partitions = ','.join(first_user_partitions)
                    new_user_partition = user_name + " gets partition " + new_user_partition + " for topic " + topic_name
                    first_user_partitions = first_user_name + " now gets partition " + first_user_partitions + " for topic " + topic_name
                    return new_user_partition + first_user_partitions

def publish_topic(topic_name,key,value,partition):
    reply_from_server = ''
    for topic in data_struct:
        if topic_name == topic[0] and int(partition) == topic[1]:
            topic[2].append([key, int(value)])
            reply_from_server += 'put '+'("'+key+", "+ value +'")'+ ' on topic '+ topic[0]+ " and partition "+ str(topic[1])
        if topic_name == topic[0] and int(partition) == -1:
            topic[2].append([key, int(value)])
            reply_from_server = 'put '+'("'+key+", "+value +'")'+ ' on topic '+ topic[0]
        return reply_from_server

def get_topic(topic_name, partition):
    reply_from_server = ''
    for topic in data_struct:
        if topic_name == topic[0] and partition==str(topic[1]):
            try:
                reply_from_server ='get'+'("'+topic[2][2][0]+", "+str(topic[2][2][1])+'")'+' from topic '+topic_name+' and partition '+str(topic[1])
                return reply_from_server
            except IndexError:
                reply_from_server = "Error: "+topic_name + " has no data left."
                return reply_from_server


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
