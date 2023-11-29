"""
 爬取韭菜公社消息
"""
import logging
import logging.handlers
import time

from bs4 import BeautifulSoup

import util.driverutil
import util.mysql
from util.kafka_producer import kafkaProducer


class new_jcgs:

    def __init__(self):
        # '构造函数'
        self.keywords = ''
        # 爬虫伪装头部设置
        # 爬虫伪装头部设置
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64; rv:51.0) Gecko/20100101 Firefox/51.0',
                        'Accept': '*/*', 'Accept-Encoding': 'gzip, deflate, br',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8', 'Connection': 'keep-alive'}  # 设置操作超时时长
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

    def parse_page(self):
        # 边界值
        url = 'https://www.jiuyangongshe.com/study_publish'
        sel = util.driverutil.get_url_content_text(url, header=self.headers)
        soup = BeautifulSoup(sel, features="html.parser")
        # 解析html
        soup_res = self.pars_soup(soup)
        # 数据落库并且发送mq
        self.add_news(soup_res)

    def add_news(self, arr):
        flag = True
        for news in arr:
            # 只爬重要新闻
            url = news['url']
            sql = "select * from monitor_news_jcgs_article where url='%s'" % (url)
            util.mysql.cur.execute(sql)
            results = util.mysql.cur.fetchall()
            if len(results) > 0:
                flag = False
                break  # 遇到重复的不在继续执行后边的
            try:
                sql = "insert into monitor_news_jcgs_article(title, digest, url,author,ctime) values('%s', '%s', '%s','%s', '%s')" % (
                    news['title'], news['digest'], news['url'], news['author'], news['ctime'])
                util.mysql.cur.execute(sql)
                util.mysql.conn.commit()
                # 爬重要新闻
                # self.kafka_op.kfk_produce_one(topic_name='new_concepts',
                #                               data_dict={'title': news['title'], 'url': news['url'],
                #                                          'description': news['digest'], 'author': news['author'],
                #                                         'time': ctime})
            except Exception as r:
                print('add monitor_news error %s' % str(r))
        return flag

    def pars_soup(self, soup):
        items = []
        m_feed_item_list = soup.select('li', atts='data-v-913ab366')
        if (len(m_feed_item_list) < 1):
            return
        m_feed_item_list = m_feed_item_list[11:]
        m_feed_item_list = m_feed_item_list[:12]
        for m_feed_item in m_feed_item_list:
            try:
                temp = dict()
                title = m_feed_item.find('div', attrs='book-title').contents[0]
                content = m_feed_item.find('div', attrs='html-text h90 htmlHile').find('a').text
                url_suffix = m_feed_item.find('div', attrs='html-text h90 htmlHile').find('a').attrs.get('href')
                name = m_feed_item.find('div', attrs='fs16-bold').text
                time = m_feed_item.find('div', attrs='fs13-ash').text
                temp['author'] = name
                temp['title'] = title
                temp['digest'] = content
                temp['ctime'] = time
                temp['url'] = 'https://www.jiuyangongshe.com' + url_suffix
                items.append(temp)
            except Exception as r:
                print('解析韭研公社html error %s' % str(r))
        return items

    def run(self):
        while True:
            print('---------执行抓取操作---------')
            self.parse_page()
            time.sleep(20)


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
    new_jcgs().run()
