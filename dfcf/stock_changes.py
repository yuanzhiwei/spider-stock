"""
#东方财富网实时交易盘口异动数据
"""
import logging
import logging.handlers
import time
from datetime import datetime

import pandas as pd
import requests

import util.driverutil
import util.mysql
import json

from data.money import intraday_money
from util.KafkaOperate import KafkaOperate
from util.util import get_code_id, trans_num


class tfcf_stock_changes:

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
        kafka_op = KafkaOperate(bootstrap_servers=bs)

    # 实时交易盘口异动数据
    def realtime_change(self, flag=None):

        '''
        flag：盘口异动类型，默认输出全部类型的异动情况
        可选：['火箭发射', '快速反弹','加速下跌', '高台跳水', '大笔买入', '大笔卖出',
            '封涨停板','封跌停板', '打开跌停板','打开涨停板','有大买盘','有大卖盘',
            '竞价上涨', '竞价下跌','高开5日线','低开5日线',  '向上缺口','向下缺口',
            '60日新高','60日新低','60日大幅上涨', '60日大幅下跌']
        '''

        # 默认输出市场全部类型的盘口异动情况（相当于短线精灵）
        changes_list = ['火箭发射', '快速反弹', '加速下跌', '高台跳水', '大笔买入',
                        '大笔卖出', '封涨停板', '封跌停板', '打开跌停板', '打开涨停板', '有大买盘',
                        '有大卖盘', '竞价上涨', '竞价下跌', '高开5日线', '低开5日线', '向上缺口',
                        '向下缺口', '60日新高', '60日新低', '60日大幅上涨', '60日大幅下跌']
        n = range(1, len(changes_list) + 1)
        change_dict = dict(zip(n, changes_list))
        if flag is not None:
            if isinstance(flag, int):
                flag = change_dict[flag]
            return self.stock_changes(symbol=flag)
        else:
            df = self.stock_changes(symbol=changes_list[0])
        for s in changes_list[1:]:
            temp = self.stock_changes(symbol=s)
            df = pd.concat([df, temp])
            df = df.sort_values('时间', ascending=False)
        return df

    def stock_changes(self, symbol):

        """
        东方财富行盘口异动
        http://quote.eastmoney.com/changes/
        :symbol:  {'火箭发射', '快速反弹', '大笔买入', '封涨停板', '打开跌停板',
                   '有大买盘', '竞价上涨', '高开5日线', '向上缺口', '60日新高',
                   '60日大幅上涨', '加速下跌', '高台跳水', '大笔卖出', '封跌停板',
                   '打开涨停板', '有大卖盘', '竞价下跌', '低开5日线', '向下缺口',
                   '60日新低', '60日大幅下跌'}
        """

        url = "http://push2ex.eastmoney.com/getAllStockChanges"

        symbol_map = {
            "火箭发射": "8201",
            "快速反弹": "8202",
            "大笔买入": "8193",
            "封涨停板": "4",
            "打开跌停板": "32",
            "有大买盘": "64",
            "竞价上涨": "8207",
            "高开5日线": "8209",
            "向上缺口": "8211",
            "60日新高": "8213",
            "60日大幅上涨": "8215",
            "加速下跌": "8204",
            "高台跳水": "8203",
            "大笔卖出": "8194",
            "封跌停板": "8",
            "打开涨停板": "16",
            "有大卖盘": "128",
            "竞价下跌": "8208",
            "低开5日线": "8210",
            "向下缺口": "8212",
            "60日新低": "8214",
            "60日大幅下跌": "8216",
        }

        reversed_symbol_map = {v: k for k, v in symbol_map.items()}
        params = {
            "type": symbol_map[symbol],
            "pageindex": "0",
            "pagesize": "5000",
            "ut": "7eea3edcaed734bea9cbfc24409ed989",
            "dpt": "wzchanges",
            "_": "1624005264245",
        }

        res = requests.get(url, params=params)
        data_json = res.json()
        df = pd.DataFrame(data_json["data"]["allstock"])
        df["tm"] = pd.to_datetime(df["tm"], format="%H%M%S").dt.time
        df.columns = ["时间", "代码", "_", "名称", "板块", "相关信息", ]
        df = df[["时间", "代码", "名称", "板块", "相关信息", ]]
        df["板块"] = df["板块"].astype(str)
        df["板块"] = df["板块"].map(reversed_symbol_map)
        return df

    # 获取个股当天实时交易快照数据
    def stock_snapshot(self, code):
        """
        获取沪深市场股票最新行情快照
        code:股票代码
        """

        code = get_code_id(code).split('.')[1]
        params = (
            ('id', code),
            ('callback', 'jQuery183026310160411569883_1646052793441'),
        )
        columns = {
            'code': '代码',
            'name': '名称',
            'time': '时间',
            'zd': '涨跌额',
            'zdf': '涨跌幅',
            'currentPrice': '最新价',
            'yesClosePrice': '昨收',
            'openPrice': '今开',
            'open': '开盘',
            'high': '最高',
            'low': '最低',
            'avg': '均价',
            'topprice': '涨停价',
            'bottomprice': '跌停价',
            'turnover': '换手率',
            'volume': '成交量',
            'amount': '成交额',
            'sale1': '卖1价',
            'sale2': '卖2价',
            'sale3': '卖3价',
            'sale4': '卖4价',
            'sale5': '卖5价',
            'buy1': '买1价',
            'buy2': '买2价',
            'buy3': '买3价',
            'buy4': '买4价',
            'buy5': '买5价',
            'sale1_count': '卖1数量',
            'sale2_count': '卖2数量',
            'sale3_count': '卖3数量',
            'sale4_count': '卖4数量',
            'sale5_count': '卖5数量',
            'buy1_count': '买1数量',
            'buy2_count': '买2数量',
            'buy3_count': '买3数量',
            'buy4_count': '买4数量',
            'buy5_count': '买5数量',
        }
        response = requests.get(
            'https://hsmarketwg.eastmoney.com/api/SHSZQuoteSnapshot', params=params)
        start_index = response.text.find('{')
        end_index = response.text.rfind('}')
        s = pd.Series(index=columns.values(), dtype='object')

        try:
            data = json.loads(response.text[start_index:end_index + 1])
        except:
            return s

        if not data.get('fivequote'):
            return s
        d = {**data.pop('fivequote'), **data.pop('realtimequote'), **data}

        ss = pd.Series(d).rename(index=columns)[columns.values()]
        str_type_list = ['代码', '名称', '时间']

        all_type_list = columns.values()

        for column in (set(all_type_list) - set(str_type_list)):
            ss[column] = str(ss[column]).strip('%')
        df = pd.DataFrame(ss).T
        # 将object类型转为数值型
        ignore_cols = ['名称', '代码', '时间']
        df = trans_num(df, ignore_cols)
        return df

    # 解析
    def parse_result(self, arr):
        sql = "select * from dfcf_stock_changes  order by gmt_created desc limit 1"
        util.mysql.cur.execute(sql)
        results = util.mysql.cur.fetchall()
        if len(results) == 1:
            self.boundary = results[0]

        times = arr['时间'];
        if (times is None):
            return
        codes = arr['代码']
        names = arr['名称']
        event = arr['板块']
        content = arr['相关信息']

        result = []
        for index in range(0, len(times)):
            # 判断当前记录是否是边界值 ,如果是边界值则中断后续操作
            if len(self.boundary) > 0 and codes[index] == self.boundary[1] and times[index] == self.boundary[
                2] and event[index] == \
                    self.boundary[3]:
                print(u'遇到边界值结束此次抓取, %s' % self.boundary)
                continue

            hit = dict()
            hit['change_time'] = times[index]
            hit['stock_code'] = codes[index]
            hit['stock_name'] = names[index]
            hit['event'] = event[index]
            hit['content'] = content[index]
            result.append(hit);
            # 入库
        self.add_news(result);
        # 判断是否需要推送至微信 主力净流入占资金总流入的10%以上
        self.is_need_push_message(result)

    def is_need_push_message(self, arr):
        if (arr is None):
            return
        # time_str = time.strftime('%H:%M:%S', time.localtime(time.time()))
        # t = pd.to_datetime(time_str, '%H:%M:%S',errors='ignore').dt.time;
        for item in arr:
            # change_time = item['change_time']
            # diff = t - change_time;
            # if (diff > 10):
            #    continue
            df = intraday_money(item['stock_code'])
            fast = df.iloc[0];
            print(fast)
            zljlr = fast['主力净流入']
            xdjlr = fast['小单净流入']
            zdjlr = fast['中单净流入']
            ddjlr = fast['大单净流入']
            cddjlr = fast['超大单净流入']
            # 小单净流入+ 中单净流入+大单净流入+超大单净流入
            cjl = xdjlr + zdjlr + ddjlr + cddjlr
            # 主力控盘力度
            zlkpld = (zljlr - cjl) % 100
            if (zlkpld >= 10):
                self.kafka_op.kfk_produce_one(topic_name='001_test',
                                              data_dict={'标题': item['event'] + '--' + item['stock_code'],
                                                         '异动时间': item['change_time'],
                                                         '主力净流入': (zljlr / 10000),
                                                         '成交量': (cjl / 10000)})

    def add_news(self, arr):
        flag = True
        for news in arr:
            sql = "select * from dfcf_stock_changes where url='%s'" % (news['url'])
            util.mysql.cur.execute(sql)
            results = util.mysql.cur.fetchall()
            if len(results) > 0:
                flag = False
                break  # 遇到重复的不在继续执行后边的
        try:
            sql = "insert into dfcf_stock_changes(is_deleted, gmt_created, gmt_modified, stock_code, change_time, event,content,stock_name) values('%s', '%s', " \
                  "'%s', '%s', '%s', now())" % (
                      False, 'now()', 'now()', news['stock_code'], news['change_time'], news['event'], news['content'],
                      news['stock_name'])
            util.mysql.cur.execute(sql)
            util.mysql.conn.commit()
        except Exception as r:
            print('add monitor_news error %s' % str(r))

        return flag

    def run(self):
        while True:
            print('---------执行抓取操作---------')
            self.realtime_change()
            time.sleep(10)


# main
if __name__ == '__main__':
    '''
    东方财富网实时交易盘口异动数据
    '''
    print('''
    ***************************************
    **         东方财富网实时交易盘口异动数据spider   **
    ***************************************
    ''')
    # print(now)
    df = tfcf_stock_changes().realtime_change(1)
    print(df)
    tfcf_stock_changes().parse_result(df)
