# SimplifiedDistributedSystem


## Technology
Python 2.7.3

## How to run my code
### Server-side
* python server.py [server_name]
* e.g. python server.py server1
* e.g. python server.py server1

### Client-side
* python client.py -add ip1 port1 ip2 port2
* e.g. python client.py -add 192.168.1.41 51827 192.168.1.41 51826

## Functionality
* create(topic=topic_name partition=partition_num)
* subscribe(topic=topic_name)
* publish(topic=topic_name key=key value=value partition=partition_index)
* get(topic=topic_name partition=partition_index)

![image](http://i.imgur.com/8BNtM3D.png)
