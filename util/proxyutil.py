# coding:utf-8
# 每次随机获取一个代理
import sys
import time
import urllib3
import requests


# 获取代理，返回{protocol:'http/https',ip:'1.2.3.4:20'}
def getproxy():
    return getdata5uproxy2()


def getcommonproxy():
    return getdata5uproxy2()


def getdata5uproxy2():
    url = 'http://api.data5u.mykrp.com/dynamic/get.html?order=3f28d2cc25a055f83293894e58dd137e&sep=3'
    res = None
    index = 1
    result = None
    while res is None:
        try:
            res = requests.get(url, timeout=5)
            result = res.text
        except Exception as r:
            print( 'get proxy error %s' % str(r))
        if index > 10:
            break
        time.sleep(1)
        index += 1
    proxy = {
        'protocol': 'http',
        'ip': result.replace("\n", "")
    }
    print( 'in data5u result=%s' % result.replace("\n", ""))
    return proxy


def getdata5uhttpsproxy():
    url = 'http://api.data5u.mykrp.com/dynamic/get.html?order=3f28d2cc25a055f83293894e58dd137e&sep=3'
    req = None
    res = None
    index = 1
    while res is None:
        try:
            res = requests.get(url, timeout=5)
            result = res.text
        except Exception as r:
            print( 'get proxy error %s' % str(r))
        if index > 10:
            break
        time.sleep(1)
        index += 1
    proxy = {
        'protocol': 'https',
        'ip': result.replace("\n", "")
    }
    print ('in data5u result=%s' % result.replace("\n", ""))
    return proxy


def getdata5uproxy():
    url = 'http://api.data5u.mykrp.com/dynamic/get.html?order=3f28d2cc25a055f83293894e58dd137e&sep=3'
    req = None
    index = 1
    while req is None:
        try:
            req = urllib3.urlopen(url, timeout=5)
            result = req.read()
        except Exception as r:
            print( 'get proxy error %s' % str(r))
        if index > 10:
            break
        time.sleep(1)
        index += 1
    proxy = {
        'protocol': 'http',
        'ip': result.replace("\n", "")
    }
    print( 'in data5u result=%s' % result.replace("\n", ""))
    return proxy


# 获取https代理，返回{protocol:'http/https',ip:'1.2.3.4:20'}
def gethttpsproxy():
    return getdata5uhttpsproxy()

