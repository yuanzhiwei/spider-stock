"""
爬取上证互动e
"""
import datetime
import hashlib
import logging
import logging.handlers
import time

import requests
from bs4 import BeautifulSoup

import util.driverutil
import util.mysql
from data.trade import stock_info, web_data
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
                sql = "insert into investor_question_answer(question, answer, name,question_md5,answer_time,stock_code) values('%s', '%s', '%s', '%s','%s','%s')" % (
                    news['question'], news['answer'], news['name'], question_md5, news['answer_time'],
                    news['stock_code'])
                util.mysql.cur.execute(sql)
                util.mysql.conn.commit()

                # 判断是否需要推送至微信 市值小于200亿&成交额大于3000万
                self.is_need_push_message(news)
            except Exception as r:
                print('add monitor_news error %s' % str(r))
        return flag

    # 判断是否需要推送至微信 市值小于200亿&成交额大于3000万
    def is_need_push_message(self, news):
        stockInfo = stock_info(news['stock_code'])
        flow_market_value = stockInfo['流通市值']
        industry = stockInfo['所处行业']
        score = 0
        # 市值大于50亿不关注
        if (flow_market_value < 5000000000):
            score += 2
        # 市值大于100亿不关注
        elif (flow_market_value < 10000000000):
            score += 1
        # 判断行业
        if (
                '半导体' in industry or '软件' in industry or '新能源' in industry or '电子元件' in industry or '医疗' in industry or '原料药' in industry):
            score += 1

        # 获取当前时间
        now_time = datetime.datetime.now()
        # 设置目标时间为下午 4 点
        target_time = now_time.replace(hour=15, minute=0, second=0, microsecond=0)

        # 当前时间大于下午3点 获取当天收盘数据
        if (now_time >= target_time):
            yesterday = now_time
        else:
            yesterday = now_time - datetime.timedelta(days=1)
        snapshot = web_data(news['stock_code'], yesterday.strftime("%Y%m%d"), yesterday.strftime("%Y%m%d"))
        # 成交额
        amount = snapshot['turnover'][0]
        # 收盘价
        price = snapshot['close'][0]
        if (price < 15):
            score += 1
        # 获取上一天成交额小于3000万不关注
        if (amount > 30000000):
            score += 2

        if (score < 4):
            return
        print(news)
        self.kafka_op.kfk_produce_one(topic_name='sz_interact',
                                      data_dict={'title': '上证互动E', 'question': news['question'],
                                                 'answer': news['answer'], 'answer_time': news['answer_time']})

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
            temp['stock_code'] = q[q.find('(') + 1:q.find(')')]
            items.append(temp)
        return items

    def flter_invalid_QA(self, arr):
        result = []
        for item in arr:
            if '股东数' in item['question'] or '股东人数' in item['question'] or '股东数量' in item[
                'question'] or '减持' in item['question'] or '连跌' in item['question'] or '股价下跌' in item[
                'question'] or '回购' in item['question']:
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
