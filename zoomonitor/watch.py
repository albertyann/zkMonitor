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
NodeConf = {}


def run():
    global Zookeeper, Config
    
    if zookeeper.OK !=  zookeeper.aget(Zookeeper, NodeConf['node'], aget):
        logging.info(node + 'node not found.')

def connection_watcher(handle,type,state, path):
    aget(handle, type,state,path)
    #print 'over....'
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


def read_conf_name(conf):
    if conf[0] == '\n':
        conf = conf[1:]
        
    i = conf.index('\n')
    h = conf[:i]
    m = conf[i+1:]
    return (h,m)

def handle(data, path):
    m = os.path.basename(path)

    

    str = ngzip.ungzip(base64.decodestring(data))

    sep = "&&&&&&"
    
    books = str.split(sep)
    length = len(books)
    
    version = books[1].split('\n')[2]
    tmp_path = proessOut(NodeConf['out'])
    
    for i in range(2,length):
        (h, m) = read_conf_name(books[i])
        
        if type(tmp_path) == list:
            for path in tmp_path:
                save(m, path+'/'+h, version)
        else:
            save(m, tmp_path+'/'+h, version)

    if type(tmp_path) == list:
        for path in tmp_path:
            saveversion(path+'/version', version)
            bakfile(path) 
    else:
        saveversion(tmp_path+'/version', version)
        bakfile(path) 


        
def saveversion(path, version):
    f = open(path, 'w')
    f.write(version)
    f.flush()
    f.close()
    
def save(data, path, version):
    dir_path = os.path.dirname(path)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path, 0777)
 
    f = open(path, 'a')
    f.write(data)
    f.flush()
    f.close()
    logging.info(os.path.basename(path))


def bakfile(path):
    global Config
    
    if not os.path.exists(path):
        return False
    
    version = 0
    if os.path.exists(path+'/version'):
        version = open(path+'/version').readline()

    bak_path = os.path.dirname(path)+'/bak'

    bak_ext = '.tar.gz'
    
    if not os.path.exists(bak_path):
        os.makedirs(bak_path, 0777)
        
    t = time.strftime("%Y%m%d%H%M%S", time.localtime())

    bname = '-'.join([t, str(version)])
    
    tar_path = bak_path+'/'+bname + bak_ext

    baklist = filter((lambda x: x[-7:] == bak_ext), os.listdir(bak_path))
    maxnum = int(Config['baknum'])
    bakcount = len(baklist)
    sub = bakcount - maxnum

    if sub >= 0:
        baklist.sort()
        while(sub >= 0):
            _del = baklist[sub]
            os.remove(bak_path+'/'+_del)
            sub -= 1
            
        
    
    if os.path.exists(tar_path):
        return True

    tar_cmd = 'tar czf '+tar_path+' '+path+'* --exclude bak'
    #os.system('tar czvf '+tar_path+' '+path+'/* > /dev/null 2>&1')
    os.system(tar_cmd)
    logging.info('bak '+bname)

def main():
    global Zookeeper, Config, NodeConf
    
    logging.basicConfig(filename=__log__,level=logging.DEBUG,
                        format = "[%(asctime)s] %(levelname)-8s %(message)s")
    
    
    if(os.path.exists(__sysconfig__)):
        sysconf = ConfigParser.ConfigParser()
        sysconf.read(__sysconfig__)

        Config = dict(sysconf.items('system'))

        zk_log_path = os.path.dirname(Config['zklog'])

        if not os.path.exists(zk_log_path):
            os.makedirs(zk_log_path)

        zklog = open(Config['zklog'],"w")
        zookeeper.set_log_stream(zklog)
        
        Zookeeper = zookeeper.init(Config['host']+':'+Config['port'], connection_watcher, 10000)
    else:
        logging.info('not found '+__sysconfig__)
        sys.exit()


    if(os.path.exists(__nodeconfig__)):
        nodeconf = ConfigParser.ConfigParser()
        nodeconf.read(__nodeconfig__)
        NodeConf = dict(nodeconf.items('config'))
    else:
        logging.info('not found '+__sysconfig__)
        sys.exit()
        
    logging.info('zookeeper warcher start...')


    t = threading.Thread(target=run, args=[])
    t.start()
    t.join()



    while True:
        sleep(86400)


if __name__ == '__main__':
    main();

