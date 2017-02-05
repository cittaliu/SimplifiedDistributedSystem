import socket
import sys
import argparse

data = []

def add():
    global name
    name = socket.gethostname()
    parser = argparse.ArgumentParser()
    parser.add_argument("-a","--add",nargs="+",help="Connect to server")
    args = parser.parse_args()
    ip1 = str(args.add[0])
    port1 = int(args.add[1])
    ip2 = str(args.add[2])
    port2 = int(args.add[3])
    global s1
    global s2
    s1 = socket.socket()
    s1.connect((ip1, port1))
    s2 = socket.socket()
    s2.connect((ip2, port2))
    print "[*]",name,"is connecting to the server", ip1,":",port1
    print "[*]",name,"is connecting to the server", ip2,":",port2

def create(topic, partition='1'):
    data = [topic, partition, '', '0']

    return data


def subscribe():
    return "subscribe topic"

def publish():
    return "publish topic"

def Main():
    add()

    function_dict = {'create':create, 'subscribe':subscribe, 'publish':publish}
    command = raw_input(name+"> ")
    while command != 'q':
        key = command.split('(',1)[0].strip()
        print key
        if key == "create":
            command = command.split('(', 1)[1].split(')')[0]
            feedback = dict(s.split('=', 1) for s in command.split())

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
