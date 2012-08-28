#!/usr/bin/env python
import sys
import os
import threading
import logging
import ConfigParser
from time import ctime,sleep
import zookeeper

__version__ = '0.0.1'
__sysconfig__ = 'watch.ini'
__nodeconfig__ = 'nodes.ini'
__log__ = 'watch.log'
Zookeeper = None
Config = {}


def run(node='', out=''):
    global Zookeeper, Config
    
    if zookeeper.OK !=  zookeeper.aget(Zookeeper, node, aget)
        logging.info(node + 'node not found.')

def connection_watcher(handle,type,state, path):
    #print type
    pass


def aget(zh, type, state, path):
    #print path,type, state
    if type == 3:
        (data, sate) = zookeeper.get(zh, path, aget)
        logging.info(data)
    

def main():
    global Zookeeper, Config
    
    logging.basicConfig(filename=__log__,level=logging.DEBUG,
                        format = "[%(asctime)s] %(levelname)-8s %(message)s")
    
    
    if(os.path.exists(__sysconfig__)):
        sysconf = ConfigParser.ConfigParser()
        sysconf.read(__sysconfig__)

        Config['host'] = sysconf.get('system', 'host')
        Config['port'] = sysconf.get('system', 'port')
        Config['zklog'] = sysconf.get('system', 'zklog')

        zklog = open(Config['zklog'],"w")
        zookeeper.set_log_stream(zklog)
        
        Zookeeper = zookeeper.init(Config['host']+':'+Config['port'], connection_watcher, 10000)
    else:
        logging.info('not found '+__sysconfig__)
        sys.exit()


    if(os.path.exists(__nodeconfig__)):
        nodeconf = ConfigParser.ConfigParser()
        nodeconf.read(__nodeconfig__)
    else:
        logging.info('not found '+__sysconfig__)
        sys.exit()
        
    logging.info('zookeeper warcher start...')


    i = 1;
    for n in nodeconf.sections():
        i += 1
        node = nodeconf.get(n, 'node')
        out = nodeconf.get(n, 'out')
        
        t = threading.Thread(target=run, args=(node, out))
        t.start()
        t.join()


    while True:
        sleep(86400)

        
    


if __name__ == '__main__':
    main();
