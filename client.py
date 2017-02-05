import socket
import sys
import argparse

data = []

def add():
    global name
    # connected to both servers
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

# def assign_partition(topic,partition_amount):
#     partition_amount = int(partition_amount)
#     all_partition = list(range(partition_amount))
#     data_server0 = []
#     data_server1 = []
#     for each_partiton in all_partition:
#         single_partition_record=create(topic,str(each_partiton))
#         if each_partiton %2 ==0:
#             data_server0.append(single_partition_record)
#             print data_server0
#         else:
#             data_server1.append(single_partition_record)
#             print data_server1

def create(topic, partition_index='0'):
    data = [topic, partition, '', '0']

    return data


def subscribe():
    return "subscribe topic"

def publish():
    return "publish topic"

def Main():
    # add servers to start the program
    add()
    # built a function dictionary to store all the functions
    function_dict = {'create':create, 'subscribe':subscribe, 'publish':publish}
    command = raw_input(name+"> ")
    while command != 'q':
        #get the function by getting the first word from user inputs
        key = command.split('(',1)[0].strip()
        print key
        if key == "create":
            # Parsing the command into a object which can be stored in database later
            # e.g. {'topic':'topic_1', 'partition':'2'}
            command = command.split('(', 1)[1].split(')')[0]
            feedback = dict(s.split('=', 1) for s in command.split())
            # track the value from key and call the create function
            result = create(feedback['topic'], feedback['partition'])
            print "[*] Sending to server with",key,"request", result
            result.append(key)
            send_to_server = ','.join(result)
            print send_to_server

            s1.send(send_to_server)
        command = raw_input(name+">")

    s1.close()
    while True:
        data = s1.recv(1024)
        print 'Received from server: ' + str(data)

    # data = s1.recv(1024)

    s1.close()
    s2.close()

if __name__ == '__main__':

    Main()
