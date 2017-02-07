import socket
import sys
import argparse
import pickle

global data_send_to_server1
data_send_to_server1= []
global data_send_to_server2
data_send_to_server2 = []

def add():
    global name
    name = socket.gethostname()
    parser = argparse.ArgumentParser()
    parser.add_argument("-a","--add",nargs="+",help="Connect to server")
    args = parser.parse_args()
    ip1 = str(args.add[0])
    port1 = int(args.add[1])
    # ip2 = str(args.add[2])
    # port2 = int(args.add[3])
    global s1
    # global s2
    s1 = socket.socket()
    s1.connect((ip1, port1))
    # s2 = socket.socket()
    # s2.connect((ip2, port2))
    print "[*]",name,"is connecting to the server", ip1,":",port1
    # print "[*]",name,"is connecting to the server", ip2,":",port2

def filter_data(topic, partition='1'):
    #reset the database to empyty before processing the data
    data_send_to_server1 = []
    data_send_to_server2 = []
    all_partitions = list(range(int(partition)))
    print all_partitions
    for each_partition in all_partitions:
        if each_partition%2 == 0:
            # data[0] is topic name; data[1] is partition index; data[2] is key & value pair
            data = [topic, each_partition,['', 0]]
            data_send_to_server1.append(data)
        elif each_partition%2 == 1:
            data = [topic, each_partition, ['', 0]]
            data_send_to_server2.append(data)
    return data_send_to_server1, data_send_to_server2


def subscribe(topic):
    data_send_to_server1 = []
    data_send_to_server2 = []
    data_send_to_server1 = [topic, name]
    data_send_to_server2 = [topic, name]
    return data_send_to_server1, data_send_to_server2


def publish(topic,key,value,partition='-1'):
    # Set partition = -1 when the client doesn't specify which partition he wants to publish to
    data_send_to_server1 = []
    data_send_to_server2 = []
    if partition == '-1':
        data_send_to_server1=[topic,[key,value],partition]
        data_send_to_server1=[topic,[key,value],partition]
    elif int(partition)%2 == 0:
        data_send_to_server1=[topic,[key,value],partition]
    else:
        data_send_to_server2=[topic,[key,value],partition]
    return data_send_to_server1, data_send_to_server2

def get(topic, partition):
    data_send_to_server1 = []
    data_send_to_server2 = []
    if int(partition)%2 == 0:
        data_send_to_server1=[topic,partition]
    else:
        data_send_to_server2=[topic,partition]
    return data_send_to_server1, data_send_to_server2


def Main():
    add()

    function_dict = {'filter_data':filter_data, 'subscribe':subscribe, 'publish':publish, 'get':get}
    command = raw_input(name+"> ")
    while command != 'q':
        method = command.split('(',1)[0].strip()
        # Extract the input values and generate it into object
        command = command.split('(', 1)[1].split(')')[0]
        feedback = dict(s.split('=', 1) for s in command.split())

        if method == "create":
            # Format the data and prepare the data sent to both servers
            server1_data, server2_data = filter_data(feedback['topic'], feedback['partition'])
            global num_partition
            num_partition = feedback['partition']
            server1_data.append(num_partition)
            server2_data.append(num_partition)

        elif method == "subscribe":
            server1_data, server2_data = subscribe(feedback['topic'])
        elif method == "publish":
            try:
                server1_data, server2_data = publish(feedback['topic'], feedback['key'], feedback['value'], feedback['partition'])
            except KeyError:
                server1_data, server2_data = publish(feedback['topic'], feedback['key'], feedback['value'])
        elif method == "get":
            server1_data, server2_data = get(feedback['topic'], feedback['partition'])
        else:
             command = raw_input(name+">")
             continue

        # Add the method at the end of array for letting server knows which method client is calling
        server1_data.append(method)
        server2_data.append(method)
        # Serialize the data into a string for sending preparation
        print "[*] Sending to server1 with",method,"request", server1_data
        # print "[*] Sending to server2 with",key,"request", server2_data
        server1_ready = pickle.dumps(server1_data)
        # server2_ready = pickle.dumps(server2_data)
        # Send the data to both servers

        s1.send(server1_ready)
        data = s1.recv(1024)
        print '[*] Received from server: ' + str(data)
        command = raw_input(name+">")

    s1.close()
    # s2.close()

if __name__ == '__main__':

    Main()
