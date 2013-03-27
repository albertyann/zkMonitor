#!/usr/bin/env python

import StringIO
import gzip


def ngzip(str=''):
    if len(str) > 0:
        o = StringIO.StringIO()
        gziper = gzip.GzipFile(fileobj=o, mode='wb')
        gziper.write(str)
        gziper.close()
        return o.getvalue()
    return False

def ungzip(obj=None):
    if obj is None:
        return False

    try:
        o = StringIO.StringIO(obj)    
        gziper = gzip.GzipFile(fileobj=o, mode='rb')
        str = gziper.read()
        gziper.close()
        return str
    except:
        return obj
