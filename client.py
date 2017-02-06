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
    data_send_to_server2 = []
    all_partitions = list(range(int(partition)))
    print all_partitions
    for each_partition in all_partitions:
        if each_partition%2 == 0:
            data = [topic, each_partition, '', 0]
            data_send_to_server1.append(data)
        elif each_partition%2 == 1:
            data = [topic, each_partition, '', 0]
            data_send_to_server2.append(data)
    print "Data Stored in Server1",data_send_to_server1
    print "Data Stored in Server2",data_send_to_server2
    return data_send_to_server1, data_send_to_server2


def subscribe():
    return "subscribe topic"

def publish():
    return "publish topic"

def Main():
    add()

    function_dict = {'filter_data':filter_data, 'subscribe':subscribe, 'publish':publish}
    command = raw_input(name+"> ")
    while command != 'q':
        key = command.split('(',1)[0].strip()
        print key
        if key == "create":
            command = command.split('(', 1)[1].split(')')[0]
            feedback = dict(s.split('=', 1) for s in command.split())

            server1_data, server2_data = filter_data(feedback['topic'], feedback['partition'])

            print "[*] Sending to server1 with",key,"request", server1_data
            print "[*] Sending to server2 with",key,"request", server2_data
            # Add the method at the end of array for letting server knows which method client is calling
            server1_data.append(key)
            server2_data.append(key)
            # Serialize the data into a string for sending preparation
            server1_ready = pickle.dumps(server1_data)
            server2_ready = pickle.dumps(server2_data)

            #
            s1.send(server1_ready)
        command = raw_input(name+">")

    s1.close()
    while True:
        data = s1.recv(1024)
        print 'Received from server: ' + str(data)

    # data = s1.recv(1024)

    # s1.close()
    # s2.close()

if __name__ == '__main__':

    Main()
