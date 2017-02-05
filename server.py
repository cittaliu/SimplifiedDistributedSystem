import socket
import argparse
from thread import *

database = []

def clientthread(conn):
    #infinite loop so that function do not terminate and thread do not end.
    while True:
        #Sending message to connected client
        #Receiving from client
        data = conn.recv(1024) # 1024 stands for bytes of data to be received
        print data

def Main():
    name = socket.gethostname()
    host = socket.gethostbyname(name)
    server = socket.socket()
    server.bind(('', 0))
    server.listen(5)
    port = server.getsockname()[1]
    print "[*]",args.name,"started listening on ",host,":",port
    #
    # client,addr = server.accept()
    # print "[*] Got a connection from ", addr[0],":",addr[1]
    #
    conn, addr = server.accept()
    print "[*] Got a connection from ", addr[0],":",addr[1]
    #Creating new thread. Calling clientthread function for this function and passing conn as argument.
    start_new_thread(clientthread,(conn,)) #start new thread takes 1st argument as a function name to be run, second is the tuple of arguments to the function.

    while True:
        #Accepting incoming connections
    	data = conn.recv(1024)
        data = data.split(',')
        print "[*] Received '",data,"' from the client"
        print "		Processing data"

        key = data[-1]
        data.pop()


    	if(data=="hello server"):
    	  conn.send("Hello client")
    	  print "	Processing done, data was valid.\n[*] Reply sent"
        elif(key=="create"):
    	  database.append(data)
          print database
    	elif(data=="disconnect"):
    	  conn.send("Goodbye")
    	  conn.close()
    	  break;
    	else:
    	  conn.send("Invalid Request")
    	  print "	Processing done.\n[*] Reply sent"


    conn.close()
    server.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("name", help = "Server Name")

    args = parser.parse_args()
    Main()
