#!/usr/bin/env python
import sys
import os
import threading
import logging
import ConfigParser
from time import ctime,sleep
import time
import zookeeper

#
import ngzip
import base64

__version__ = '0.0.1'
__sysconfig__ = 'watch.ini'
__nodeconfig__ = 'nodes.ini'
__log__ = 'watch.log'
Zookeeper = None
Config = {}
Play = {}


def run(i, conf=None):
    global Zookeeper, Config
    
    if zookeeper.OK !=  zookeeper.aget(Zookeeper, conf['node'], aget):
        logging.info(node + 'node not found.')

def connection_watcher(handle,type,state, path):
    #print type
    pass


def aget(zh, type, state, path):
    #print path,type, state
    if type == 3:
        (data, sate) = zookeeper.get(zh, path, aget)
        logging.info(path+' data change..')
        handle(data, path)

def proessOut(obj):
    if obj[0] == '[' and obj[-1] == ']':
        return eval(obj)
    if (obj[0] == '[' and obj[-1] != ']') or (obj[0] != '[' and obj[-1] == ']'):
        raise IOException('nodes.ini error:'+obj+'. missing \'[\' or \']\' ')

    return obj
        
def handle(data, path):
    m = os.path.basename(path)
    conf = Play[m]
    #a.say()

    str = ngzip.ungzip(base64.decodestring(data))
    
    
    tmp_path = proessOut(conf['out'])

    
    
    if type(tmp_path) == list:
        for path in tmp_path:
            save(str, path)
    else:
        save(str, tmp_path)

def save(data, path):
    dir_path = os.path.dirname(path)
    if not os.path.exists(dir_path):
        os.mkdir(dir_path, 0777)

    bakfile(path) #
    
    print path
    
    f = open(path, 'w')
    f.write(data)
    f.flush()
    f.close()
    logging.info(os.path.basename(path))


def bakfile(path):
    global Config
    
    if not os.path.exists(path):
        return False
    
    dir_path = os.path.dirname(path)
    basename = os.path.basename(path)
    version = 0
    if os.path.exists(dir_path+'/version'):
        version = open(dir_path+'/version').readline()

    bak_path = os.path.dirname(dir_path)+'/bak'
    bak_ext = '.tar.gz'
    
    if not os.path.exists(bak_path):
        os.mkdir(bak_path, 0777)
        
    t = time.strftime("%Y%m%d%H%M%S", time.localtime())

    bname = '-'.join([str(version), t])
    
    tar_path = bak_path+'/'+bname + bak_ext

    baklist = filter((lambda x: x[-7:] == bak_ext), os.listdir(bak_path))
    maxnum = int(Config['baknum'])
    bakcount = len(baklist)
    sub = bakcount - maxnum
    print sub
    if sub >= 0:
        baklist.sort()
        while(sub >= 0):
            _del = baklist[sub]
            print bak_path+'/'+_del
            os.remove(bak_path+'/'+_del)
            sub -= 1
            
        
    
    if os.path.exists(tar_path):
        return True
    
    os.system('tar czvf '+tar_path+' '+dir_path+'/* > /dev/null 2>&1')

    logging.info('bak '+basename)

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
        Config['baknum'] = sysconf.get('system', 'baknum')

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
        conf = dict(nodeconf.items(n))

        m = os.path.basename(conf['node'])
        Play[m] = conf


        
        t = threading.Thread(target=run, args=(i, conf))
        t.start()
        t.join()


    while True:
        sleep(86400)


if __name__ == '__main__':
    main();

