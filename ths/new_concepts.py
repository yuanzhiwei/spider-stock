#!/usr/bin/python
# coding: utf-8

"""
爬取同花顺新增概念
"""
import time
import datetime
import logging
import logging.handlers
import sys

import util.driverutil
import util.mysql
from util.kafka_producer import kafkaProducer


class ths_spider:
    def __init__(self):
        # '构造函数'
        self.keywords = ''
        # 爬虫伪装头部设置
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64; rv:51.0) Gecko/20100101 Firefox/51.0'}
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
        sql = "select word ,exclude_word, delete_word from monitor_keyword"
        util.mysql.cur.execute(sql)
        results = util.mysql.cur.fetchall()
        for row in results:
            self.keywords[row[0]] = [row[1], row[2]]

        self.boundary = []

        bs = 'localhost:9092'
        self.kafka_op = kafkaProducer(bootstrap_servers=bs)

    def run(self):
        while True:
            print('---------执行抓取操作---------')
            self.parse_page()
            time.sleep(20)

    def parse_page(self, page=1):
        if page == 1:
            # 获取边界表中的最新一条记录
            util.mysql.cur.execute('select * from monitor_boundary order by create_time desc limit 1')
            results = util.mysql.cur.fetchall()
            if len(results) == 1:
                self.boundary = results[0]

        url = 'http://stock.10jqka.com.cn/companynews_list/index.shtml'
        if page != 1 and page < 21:
            url = 'http://stock.10jqka.com.cn/companynews_list/index_%d.shtml' % page
        elif page >= 21:
            return

        print('##################开始抓取: %d####################' % page)
        sel = util.driverutil.get_url_content_with_proxy(url)
        if sel is None:
            print('sel is None, try again')
            self.parse_page(page)
            return

        result = []
        # 边界值
        boundary_flag = False
        for index in range(len(sel.xpath("//div[@class='list-con']/ul/li"))):
            li = sel.xpath("//div[@class='list-con']/ul/li")[index]
            aurl = li.xpath("./span/a/@href")[0]
            title = li.xpath("./span/a")[0].text
            time = li.xpath("./span/span")[0].text
            description = li.xpath("./a")[0].text

            # 判断当前记录是否是边界值 ,如果是边界值则中断后续操作
            if len(self.boundary) > 0 and title == self.boundary[1] and aurl == self.boundary[2] and time == \
                    self.boundary[3]:
                self.logger.info(u'遇到边界值结束此次抓取: {0}'.format(self.boundary))
                boundary_flag = True
                break
            # 记录当前最新的一条数据
            if page == 1 and index == 0:
                try:
                    sql = "insert into monitor_boundary(title, url, time, create_time) values('%s', '%s', " \
                          "'%s', now())" % (title, aurl, time)
                    util.mysql.cur.execute(sql)
                    util.mysql.conn.commit()
                    self.logger.info(u'新增最新爬取数据为边界值：{0}'.format(title))
                except Exception as r:
                    print('add monitor_news error %s' % str(r))

            # 遍历关键字匹配
            for key in self.keywords:  # key 关键字 v0 排除关键字 v1删除关键字
                if key in title:
                    if self.keywords[key][0] is None or len(self.keywords[key][0]) == 0:
                        hit = dict()
                        if self.keywords[key][1] is not None:
                            deletes = self.keywords[key][1].split(',')
                            for item in deletes:
                                title = title.replace(item, '')
                        hit['title'] = title
                        hit['url'] = aurl
                        hit['description'] = description
                        hit['keyword'] = key
                        hit['time'] = time
                        result.append(hit)
                        self.logger.info(u'监控到新增概念1：{0}'.format(hit))
                    else:
                        for item in self.keywords[key].split(','):
                            if item in title:
                                break
                        else:
                            hit = dict()
                            hit['title'] = title
                            hit['url'] = aurl
                            hit['description'] = description
                            hit['keyword'] = key
                            hit['time'] = time
                            result.append(hit)
                            self.logger.info(u'监控到新增概念2：{0}'.format(hit))
        if len(result) > 0:
            self.logger.info(u'执行消息推送：{0}'.format(result))
            flag = self.add_news(result)
            if flag and not boundary_flag:  # 如果正常插入且循环没有遇到边界值
                self.parse_page(page + 1)
        elif not boundary_flag:
            self.parse_page(page + 1)

    def add_news(self, arr):
        flag = True
        for news in arr:
            sql = "select * from monitor_news where url='%s'" % (news['url'])
            util.mysql.cur.execute(sql)
            results = util.mysql.cur.fetchall()
            if len(results) > 0:
                flag = False
                break  # 遇到重复的不在继续执行后边的
            try:
                sql = "insert into monitor_news(title, url, description, keyword, time, create_time) values('%s', '%s', " \
                      "'%s', '%s', '%s', now())" % (
                          news['title'], news['url'], news['description'], news['keyword'], news['time'])
                util.mysql.cur.execute(sql)
                util.mysql.conn.commit()

                self.kafka_op.kfk_produce_one(topic_name='new_concepts',
                                              data_dict={'title': news['title'], 'url': news['url'],
                                                         'description': news['description'], 'time': news['time']})
            except Exception as r:
                print('add monitor_news error %s' % str(r))
        return flag


# main
if __name__ == '__main__':
    '''
    新增概念
    '''
    print('''
    ***************************************
    **           同花顺监控spider          **
    ***************************************
    ''')
    ths_spider().run()
