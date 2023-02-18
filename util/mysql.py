#!/usr/bin/python
# coding:utf-8
# 获取mysql连接，统一都用这个，引用时通过import util.mysql，使用时也是util.mysql.conn或util.mysql.cur

import configparser
import pymysql
pymysql.install_as_MySQLdb()


config = configparser.ConfigParser()
config.read("../config.properties")
session_name = 'local'

conn = pymysql.connect(
    host=config[session_name]['local_host'],
    port=3306,
    user=config[session_name]['local_username'],
    passwd=config[session_name]['local_password'],
    db=config[session_name]['local_db'],
    charset='utf8')

conn.ping(True)

cur = conn.cursor()
