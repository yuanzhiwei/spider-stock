"""
爬取上证互动e
"""
import time
import datetime
import logging
import logging.handlers
import hashlib
import sys

import requests
from bs4 import BeautifulSoup

import util.driverutil
import util.mysql
from util.kafka_producer import kafkaProducer


class sz_interact_spider:
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

        self.boundary = []
        bs = 'localhost:9092'
        self.kafka_op = kafkaProducer(bootstrap_servers=bs)

    def parse_page(self, page=1):
        # 边界值
        if page == 1:
            url = 'http://sns.sseinfo.com/ajax/feeds.do?page=1&type=11&pageSize=10&lastid=-1&show=1&_=%d' % datetime.datetime.now().timestamp()
        if page != 1 and page < 21:
            url = 'http://sns.sseinfo.com/ajax/feeds.do?page=%d&type=11&pageSize=10&lastid=-1&show=1&_=%d' % (
                page, datetime.datetime.now().timestamp())
        elif page >= 21:
            return
        print('##################开始抓取: %d####################' % page)
        sel = requests.get(url)
        soup = BeautifulSoup(sel.text, features="html.parser")
        # 解析html
        soup_res = self.pars_soup(soup)

        res = self.flter_invalid_QA(soup_res)
        if len(res) > 0:
            self.logger.info(u'执行消息推送：{0}'.format(res))
            flag = self.add_news(res)
            if flag:  # 如果正常插入且循环没有遇到边界值
                self.parse_page(page + 1)

    def add_news(self, arr):
        flag = True
        for news in arr:
            question = news['question']
            question_md5 = self.md5_encrypt(question)
            sql = "select * from investor_question_answer where question_md5='%s'" % (question_md5)
            util.mysql.cur.execute(sql)
            results = util.mysql.cur.fetchall()
            if len(results) > 0:
                flag = False
                break  # 遇到重复的不在继续执行后边的
            try:
                sql = "insert into investor_question_answer(question, answer, name,question_md5,answer_time) values('%s', '%s', '%s', '%s','%s')" % (
                    news['question'], news['answer'], news['name'], question_md5, news['answer_time'])
                util.mysql.cur.execute(sql)
                util.mysql.conn.commit()

                self.kafka_op.kfk_produce_one(topic_name='sz_interact',
                                              data_dict={'title': '上证互动E', 'question': news['question'],
                                                         'answer': news['answer'], 'answer_time': news['answer_time']})
            except Exception as r:
                print('add monitor_news error %s' % str(r))
        return flag

    def run(self):
        while True:
            print('---------执行抓取操作---------')
            self.parse_page()
            time.sleep(60)

    def md5_encrypt(self, str):
        md5 = hashlib.md5()
        md5.update(str.encode("utf-8"))
        return md5.hexdigest()

    def pars_soup(self, soup):

        items = []

        m_feed_item_list = soup.find_all('div', 'm_feed_item')
        for m_feed_item in m_feed_item_list:
            temp = dict()

            id = m_feed_item.get('id')
            name = m_feed_item.find('p').text
            qa = m_feed_item.find_all('div', 'm_feed_txt')

            time = m_feed_item.find_all_next('div', 'm_feed_from')[1].text.replace('\n', '').replace('来自网站', '')
            q = qa[0].text.strip()
            if len(qa) == 2:
                a = qa[1].text.strip()
            else:
                a = ''
            temp['id'] = id
            temp['name'] = name
            temp['question'] = q
            temp['answer'] = a
            temp['answer_time'] = time
            items.append(temp)

        return items

    def flter_invalid_QA(self, arr):
        result = []
        for item in arr:
            if '股东数' in item['question']:
                continue
            result.append(item)
        return result


# main
if __name__ == '__main__':
    '''
    新增概念
    '''
    print('''
    ***************************************
    **           上证互动E监控spider          **
    ***************************************
    ''')
    sz_interact_spider().run()
