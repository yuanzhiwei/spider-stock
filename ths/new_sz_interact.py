"""
爬取深交所互动e
"""
import datetime
import hashlib
import logging
import logging.handlers
import time

import requests

import util.driverutil
import util.mysql
from data.trade import stock_info, web_data
from util.kafka_producer import kafkaProducer


class sz_interact_spider:

    def __init__(self):
        # '构造函数'
        self.keywords = ''
        # 爬虫伪装头部设置
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64; rv:51.0) Gecko/20100101 Firefox/51.0',
                        'sendType': 'formdata'}
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
        url = 'http://irm.cninfo.com.cn/newircs/index/search?_t=%d' % (datetime.datetime.now().timestamp() / 1000)
        print('##################开始抓取: %d####################' % page)
        params = {'pageNo': page, 'pageSize': 10, 'searchType': 11, 'highLight': True}
        sel = requests.post(url=url, data=params, headers=self.headers)
        resp = sel.json()
        list = resp['results']
        res = self.flter_invalid_QA(list)
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
                    news['stockCode'])
                util.mysql.cur.execute(sql)
                util.mysql.conn.commit()

                # 判断是否需要推送至微信 市值小于200亿&成交额大于3000万
                self.is_need_push_message(news)
            except Exception as r:
                print('add monitor_news error %s' % str(r))
        return flag

    def md5_encrypt(self, str):
        md5 = hashlib.md5()
        md5.update(str.encode("utf-8"))
        return md5.hexdigest()

    def flter_invalid_QA(self, list):
        result = []
        for item in list:
            if '股东数' in item['mainContent'] or '股东人数' in item['mainContent'] or '股东数量' in item[
                'mainContent'] or '减持' in item['mainContent'] or '连跌' in item['mainContent'] or '股价下跌' in item[
                'mainContent'] or '回购' in item['mainContent']:
                continue
            element = {'name': item['authorName'], 'stockCode': item['stockCode'],
                       'question': item['mainContent'],
                       'answer': item['attachedContent'], 'answer_time': item['updateDate']}
            result.append(element)
        return result

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
        # target_time = now_time.replace(hour=15, minute=0, second=0, microsecond=0)

        # 当前时间大于下午3点 获取当天收盘数据
        yesterday = now_time - datetime.timedelta(days=14)

        snapshot_list = web_data(news['stock_code'], yesterday.strftime("%Y%m%d"), now_time.strftime("%Y%m%d"))
        # 判断价格
        for row in snapshot_list.itertuples():
            # 价格
            price_fluctuations = row[10]
            if (price_fluctuations >= 9.5):
                score += 2
                break
        snapshot_item = snapshot_list.tail(1)
        # 成交额
        amount = snapshot_item['turnover'][0]
        # 收盘价
        price = snapshot_item['close'][0]
        if (price < 15):
            score += 1
        # 获取上一天成交额小于3000万不关注
        if (amount > 30000000):
            score += 2
        if (score < 4):
            return
        print(news)
        self.kafka_op.kfk_produce_one(topic_name='sz_interact',
                                       data_dict={'title': '深交所互动E', 'question': news['question'],
                                                  'answer': news['answer'], 'answer_time': news['answer_time']})

    def run(self):
        while True:
            print('---------执行抓取操作---------')
            self.parse_page()
            time.sleep(60)


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
