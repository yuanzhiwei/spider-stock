#!/usr/bin/python
# coding: utf-8

"""
爬取同花顺重要新闻
"""
import logging
import logging.handlers
import time

import requests

import util.driverutil
import util.mysql
from util.kafka_producer import kafkaProducer


class ths_new_inportant:
    def __init__(self):
        # '构造函数'
        self.keywords = ''
        # 爬虫伪装头部设置
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64; rv:51.0) Gecko/20100101 Firefox/51.0',
                        'Accept': '*/*', 'Accept-Encoding': 'gzip, deflate, br',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8', 'Connection': 'keep-alive'}
        # 设置操作超时时长
        self.timeout = 5
        # webdriver
        self.browser = None
        self.index = 0
        # logging
        LOG_FILE = time.strftime('%Y-%m-%d') + '.log'
        handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=1024 * 1024, backupCount=5)  # 实例化handler
        fmt = '%(asctime)s %(filename)s:%(lineno)s %(levelname)s %(message)s'
        ch = logging.StreamHandler()
        formatter = logging.Formatter(fmt)  # 实例化formatter
        handler.setFormatter(formatter)  # 为handler添加formatter
        ch.setFormatter(formatter)

        self.logger = logging.getLogger(time.strftime('%Y-%m-%d'))  # 获取名为日期的logger
        self.logger.addHandler(handler)  # 为logger添加handler
        self.logger.addHandler(ch)
        self.logger.setLevel(logging.DEBUG)

        self.keywords = dict()

        bs = 'localhost:9092'
        self.kafka_op = kafkaProducer(bootstrap_servers=bs)

    def run(self):
        while True:
            print('---------执行抓取操作---------')
            self.parse_page()
            time.sleep(20)

    def parse_page(self, page=1):
        url = 'http://news.10jqka.com.cn/tapp/news/push/stock/?page=1&tag=-21101&track=website&pagesize=5'
        res = requests.get(url, headers=self.headers).json()

        arrays = res['data']['list']
        if len(arrays) > 0:
            flag = self.add_news(arrays)
            if flag:  # 如果正常插入且循环没有遇到边界值
                self.parse_page(page + 1)

    def flter_invalid_QA(self, arr):
        result = []
        for item in arr:
            if item['color'] in '2':
                result.append(item)
        return result

    def add_news(self, arr):
        flag = True
        for news in arr:
            # 只爬重要新闻
            url = news['url']
            sql = "select * from monitor_news_important where url='%s'" % (url)
            util.mysql.cur.execute(sql)
            results = util.mysql.cur.fetchall()
            if len(results) > 0:
                flag = False
                break  # 遇到重复的不在继续执行后边的
            try:
                ctime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(news['ctime'])))
                sql = "insert into monitor_news_important(title, digest, url,ctime) values('%s', '%s', '%s', '%s')" % (
                    news['title'], news['digest'], news['url'], ctime)
                util.mysql.cur.execute(sql)
                util.mysql.conn.commit()
                # 爬重要新闻
                self.kafka_op.kfk_produce_one(topic_name='new_concepts',
                                              data_dict={'title': news['title'], 'url': news['url'],
                                                         'description': news['digest'], 'time': ctime})
                time.sleep(100)
            except Exception as r:
                print('add monitor_news error %s' % str(r))
        return flag


# main
if __name__ == '__main__':
    '''
    监控同花顺重要新闻
    '''
    print('''
    ***************************************
    **           监控同花顺重要新闻spider          **
    ***************************************
    ''')
    ths_new_inportant().run()
