#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Author  ：Jinyi Zhang 
@Date    ：2022/9/29 20:28 
'''
import time
import requests
import pandas as pd
from retry.api import retry
from pathlib import Path
from py_mini_racer import py_mini_racer

# 东方财富网网页请求头
request_header = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; Touch; rv:11.0) like Gecko',
    'Accept': '*/*',
    'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2'}

session = requests.Session()

trade_detail_dict = {
    'f12': '代码',
    'f14': '名称',
    'f3': '涨幅',
    'f2': '最新',
    'f15': '最高',
    'f16': '最低',
    'f17': '今开',
    'f8': '换手率',
    'f10': '量比',
    'f9': '市盈率',
    'f5': '成交量',
    'f6': '成交额',
    'f18': '昨收',
    'f20': '总市值',
    'f21': '流通市值',
    'f13': '编号',
    'f124': '更新时间戳',
}

# 市场与编码
market_dict = {
    'stock': 'm:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23',
    '沪深A': 'm:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23',
    '上证A': 'm:1 t:2,m:1 t:23',
    '沪A': 'm:1 t:2,m:1 t:23',
    '深证A': 'm:0 t:6,m:0 t:80',
    '深A': 'm:0 t:6,m:0 t:80',
    '北证A': 'm:0 t:81 s:2048',
    '北A': 'm:0 t:81 s:2048',
    '创业板': 'm:0 t:80',
    '科创板': 'm:1 t:23',
    '沪深京A': 'm:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048',
    '沪股通': 'b:BK0707',
    '深股通': 'b:BK0804',
    '风险警示板': 'm:0 f:4,m:1 f:4',
    '两网及退市': 'm:0 s:3',
    '新股': 'm:0 f:8,m:1 f:8',
    '美股': 'm:105,m:106,m:107',
    '港股': 'm:128 t:3,m:128 t:4,m:128 t:1,m:128 t:2',
    '英股': 'm:155 t:1,m:155 t:2,m:155 t:3,m:156 t:1,m:156 t:2,m:156 t:5,m:156 t:6,m:156 t:7,m:156 t:8',
    '中概股': 'b:MK0201',
    '中国概念股': 'b:MK0201',
    '地域板块': 'm:90 t:1 f:!50',
    '行业板块': 'm:90 t:2 f:!50',
    '概念板块': 'm:90 t:3 f:!50',
    '上证指数': 'm:1 s:2',
    '上证系列指数': 'm:1 s:2',
    '深证指数': 'm:0 t:5',
    '深证系列指数': 'm:0 t:5',
    '沪深指数': 'm:1 s:2,m:0 t:5',
    '沪深系列指数': 'm:1 s:2,m:0 t:5',
    'bond': 'b:MK0354',
    '债券': 'b:MK0354',
    '可转债': 'b:MK0354',
    'future': 'm:113,m:114,m:115,m:8,m:142',
    '期货': 'm:113,m:114,m:115,m:8,m:142',
    'fund': 'b:MK0021,b:MK0022,b:MK0023,b:MK0024',
    'ETF': 'b:MK0021,b:MK0022,b:MK0023,b:MK0024',
    'LOF': 'b:MK0404,b:MK0405,b:MK0406,b:MK0407', }

# 市场编号
market_num_dict = {
    '0': '深A',
    '1': '沪A',
    '105': '美股',
    '106': '美股',
    '107': '美股',
    '116': '港股',
    '128': '港股',
    '113': '上期所',
    '114': '大商所',
    '115': '郑商所',
    '8': '中金所',
    '142': '上海能源期货交易所',
    '155': '英股',
    '90': '板块'
}

code_id_dict = {
    '上证综指': '1.000001', 'sh': '1.000001', '上证指数': '1.000001', '1.000001': '1.000001',
    '深证综指': '0.399106', 'sz': '0.399106', '深证指数': '0.399106', '深证成指': '0.399106',
    '创业板指': '0.399006', 'cyb': '0.399006', '创业板': '0.399006', '创业板指数': '0.399006',
    '沪深300': '1.000300', 'hs300': '1.000300',
    '上证50': '1.000016', 'sz50': '1.000016',
    '上证180': '1.000010', 'sz180': '1.000010',
    '科创50': '1.000688', 'kc50': '1.000688',
    '中小100': '0.399005', 'zxb': '0.399005', '中小板': '0.399005', '中小板指数': '0.399005', '深圳100': '0.399005',
    '标普500': '100.SPX', 'SPX': '100.SPX', 'spx': '100.SPX', '标普指数': '100.SPX',
    '纳斯达克': '100.NDX', '纳斯达克指数': '100.NDX', 'NSDQ': '100.NDX', 'nsdq': '100.NDX',
    '道琼斯': '100.DJIA', 'DJIA': '100.DJIA', 'dqs': '100.DJIA', '道琼斯指数': '100.DJIA',
    '韩国KOSPI': '100.KS11', '韩国综合': '100.KS11', '韩国综合指数': '100.KS11', '韩国指数': '100.KS11',
    '加拿大S&P/TSX': '100.TSX', '加拿大指数': '100.TSX',
    '巴西BOVESPA': '100.BVSP', '巴西指数': '100.BVSP',
    '墨西哥BOLSA': '100.MXX', '墨西哥指数': '100.MXX',
    '俄罗斯RTS': '100.RTS', '俄罗斯指数': '100.RTS',
}

js_str = """
    function mcode(input) {  
                var keyStr = "ABCDEFGHIJKLMNOP" + "QRSTUVWXYZabcdef" + "ghijklmnopqrstuv"   + "wxyz0123456789+/" + "=";  
                var output = "";  
                var chr1, chr2, chr3 = "";  
                var enc1, enc2, enc3, enc4 = "";  
                var i = 0;  
                do {  
                    chr1 = input.charCodeAt(i++);  
                    chr2 = input.charCodeAt(i++);  
                    chr3 = input.charCodeAt(i++);  
                    enc1 = chr1 >> 2;  
                    enc2 = ((chr1 & 3) << 4) | (chr2 >> 4);  
                    enc3 = ((chr2 & 15) << 2) | (chr3 >> 6);  
                    enc4 = chr3 & 63;  
                    if (isNaN(chr2)) {  
                        enc3 = enc4 = 64;  
                    } else if (isNaN(chr3)) {  
                        enc4 = 64;  
                    }  
                    output = output + keyStr.charAt(enc1) + keyStr.charAt(enc2)  
                            + keyStr.charAt(enc3) + keyStr.charAt(enc4);  
                    chr1 = chr2 = chr3 = "";  
                    enc1 = enc2 = enc3 = enc4 = "";  
                } while (i < input.length);  
          
                return output;  
            }  
"""
random_time_str = str(int(time.time()))
js_code = py_mini_racer.MiniRacer()
js_code.eval(js_str)
mcode = js_code.call("mcode", random_time_str)

#巨潮信息网站网页请求头
cn_headers = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Content-Length": "0",
        "Host": "webapi.cninfo.com.cn",
        "mcode": mcode,
        "Origin": "http://webapi.cninfo.com.cn",
        "Pragma": "no-cache",
        "Proxy-Connection": "keep-alive",
        "Referer": "http://webapi.cninfo.com.cn/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
    }

ths_code_name = {
        "881101": "种植业与林业",
        "881102": "养殖业",
        "881103": "农产品加工",
        "881104": "农业服务",
        "881105": "煤炭开采加工",
        "881107": "油气开采及服务",
        "881108": "化学原料",
        "881109": "化学制品",
        "881110": "化工合成材料",
        "881112": "钢铁",
        "881114": "金属新材料",
        "881115": "建筑材料",
        "881116": "建筑装饰",
        "881117": "通用设备",
        "881118": "专用设备",
        "881119": "仪器仪表",
        "881120": "电力设备",
        "881121": "半导体及元件",
        "881122": "光学光电子",
        "881123": "其他电子",
        "881124": "消费电子",
        "881125": "汽车整车",
        "881126": "汽车零部件",
        "881127": "非汽车交运",
        "881128": "汽车服务",
        "881129": "通信设备",
        "881130": "计算机设备",
        "881131": "白色家电",
        "881132": "黑色家电",
        "881133": "饮料制造",
        "881134": "食品加工制造",
        "881135": "纺织制造",
        "881136": "服装家纺",
        "881137": "造纸",
        "881138": "包装印刷",
        "881139": "家用轻工",
        "881140": "化学制药",
        "881141": "中药",
        "881142": "生物制品",
        "881143": "医药商业",
        "881144": "医疗器械",
        "881145": "电力",
        "881146": "燃气",
        "881148": "港口航运",
        "881149": "公路铁路运输",
        "881151": "机场航运",
        "881152": "物流",
        "881153": "房地产开发",
        "881155": "银行",
        "881156": "保险及其他",
        "881157": "证券",
        "881158": "零售",
        "881159": "贸易",
        "881160": "景点及旅游",
        "881161": "酒店及餐饮",
        "881162": "通信服务",
        "881163": "计算机应用",
        "881164": "传媒",
        "881165": "综合",
        "881166": "国防军工",
        "881167": "非金属材料",
        "881168": "工业金属",
        "881169": "贵金属",
        "881170": "小金属",
        "881171": "自动化设备",
        "881172": "电子化学品",
        "881173": "小家电",
        "881174": "厨卫电器",
        "881175": "医疗服务",
        "881176": "房地产服务",
        "881177": "互联网电商",
        "881178": "教育",
        "881179": "其他社会服务",
        "881180": "石油加工贸易",
        "881181": "环保",
        "881182": "美容护理",
        "884001": "种子生产",
        "884002": "粮食种植",
        "884003": "其他种植业",
        "884004": "林业",
        "884005": "海洋捕捞",
        "884006": "水产养殖",
        "884007": "畜禽养殖",
        "884008": "饲料",
        "884009": "果蔬加工",
        "884010": "粮油加工",
        "884011": "其他农产品加工",
        "884012": "农业综合",
        "884013": "动物保健",
        "884014": "煤炭开采",
        "884015": "焦炭加工",
        "884016": "油气开采",
        "884018": "油服工程",
        "884020": "石油加工",
        "884021": "油品石化贸易",
        "884022": "纯碱",
        "884023": "氯碱",
        "884024": "无机盐",
        "884025": "其他化学原料",
        "884026": "氮肥",
        "884027": "磷肥及磷化工",
        "884028": "农药",
        "884030": "涂料油墨",
        "884031": "钾肥",
        "884032": "民爆用品",
        "884033": "纺织化学用品",
        "884034": "其他化学制品",
        "884035": "复合肥",
        "884036": "氟化工",
        "884039": "聚氨酯",
        "884041": "涤纶",
        "884043": "粘胶",
        "884044": "其他纤维",
        "884045": "氨纶",
        "884046": "其他塑料制品",
        "884048": "改性塑料",
        "884050": "其他橡胶制品",
        "884051": "炭黑",
        "884052": "普钢",
        "884053": "铝",
        "884054": "铜",
        "884055": "铅锌",
        "884056": "其他金属新材料",
        "884057": "磁性材料",
        "884058": "非金属材料Ⅲ",
        "884059": "玻璃玻纤",
        "884060": "水泥",
        "884062": "其他建材",
        "884063": "耐火材料",
        "884064": "管材",
        "884065": "装饰园林",
        "884066": "房屋建设",
        "884067": "基础建设",
        "884068": "专业工程",
        "884069": "机床工具",
        "884071": "磨具磨料",
        "884073": "制冷空调设备",
        "884074": "其他通用设备",
        "884075": "金属制品",
        "884076": "纺织服装设备",
        "884077": "工程机械",
        "884078": "农用机械",
        "884080": "能源及重型设备",
        "884081": "印刷包装机械",
        "884082": "其他专用设备",
        "884083": "楼宇设备",
        "884084": "环保设备",
        "884085": "电机",
        "884086": "电气自控设备",
        "884088": "输变电设备",
        "884089": "线缆部件及其他",
        "884090": "分立器件",
        "884091": "半导体材料",
        "884092": "印制电路板",
        "884093": "被动元件",
        "884094": "面板",
        "884095": "LED",
        "884096": "光学元件",
        "884098": "消费电子零部件及组装",
        "884099": "乘用车",
        "884100": "商用载货车",
        "884101": "商用载客车",
        "884105": "轨交设备",
        "884106": "其他交运设备",
        "884107": "汽车服务Ⅲ",
        "884112": "冰洗",
        "884113": "空调",
        "884115": "小家电Ⅲ",
        "884116": "其他白色家电",
        "884117": "彩电",
        "884118": "其他黑色家电",
        "884119": "其他酒类",
        "884120": "软饮料",
        "884123": "肉制品",
        "884124": "调味发酵品",
        "884125": "乳品",
        "884126": "其他食品",
        "884128": "棉纺",
        "884130": "印染",
        "884131": "辅料",
        "884132": "其他纺织",
        "884136": "鞋帽及其他",
        "884137": "家纺",
        "884139": "家具",
        "884140": "其他家用轻工",
        "884141": "饰品",
        "884142": "文娱用品",
        "884143": "原料药",
        "884144": "化学制剂",
        "884145": "医疗设备",
        "884146": "火电",
        "884147": "水电",
        "884149": "热力",
        "884150": "新能源发电",
        "884152": "燃气Ⅲ",
        "884153": "港口",
        "884154": "高速公路",
        "884155": "铁路运输",
        "884156": "机场",
        "884157": "航空运输",
        "884158": "多元金融",
        "884159": "保险",
        "884160": "百货零售",
        "884161": "专业连锁",
        "884162": "商业物业经营",
        "884163": "人工景点",
        "884164": "自然景点",
        "884165": "旅游综合",
        "884167": "酒店",
        "884168": "餐饮",
        "884172": "有线电视网络",
        "884173": "通信服务Ⅲ",
        "884174": "软件开发",
        "884176": "出版",
        "884177": "影视院线",
        "884178": "广告营销",
        "884179": "其他传媒",
        "884180": "航天装备",
        "884181": "航空装备",
        "884182": "地面兵装",
        "884183": "航海装备",
        "884184": "特钢",
        "884185": "贵金属Ⅲ",
        "884186": "其他小金属",
        "884188": "白酒",
        "884189": "啤酒",
        "884191": "航运",
        "884192": "仪器仪表Ⅲ",
        "884193": "其他电子Ⅲ",
        "884194": "汽车零部件Ⅲ",
        "884195": "造纸Ⅲ",
        "884197": "中药Ⅲ",
        "884199": "医药商业Ⅲ",
        "884200": "公交",
        "884201": "物流Ⅲ",
        "884202": "住宅开发",
        "884203": "产业地产",
        "884205": "证券Ⅲ",
        "884206": "贸易Ⅲ",
        "884207": "计算机设备Ⅲ",
        "884208": "综合Ⅲ",
        "884209": "钛白粉",
        "884210": "食品及饲料添加剂",
        "884211": "有机硅",
        "884212": "合成树脂",
        "884213": "膜材料",
        "884214": "冶钢原料",
        "884215": "稀土",
        "884216": "能源金属",
        "884217": "工程咨询服务",
        "884218": "机器人",
        "884219": "工控设备",
        "884220": "激光设备",
        "884221": "其他自动化设备",
        "884222": "光伏设备",
        "884223": "风电设备",
        "884224": "电池",
        "884225": "其他电源设备",
        "884226": "集成电路设计",
        "884227": "集成电路制造",
        "884228": "集成电路封测",
        "884229": "半导体设备",
        "884230": "品牌消费电子",
        "884231": "电子化学品Ⅲ",
        "884232": "厨卫电器Ⅲ",
        "884233": "休闲食品",
        "884234": "服装",
        "884235": "印刷",
        "884236": "包装",
        "884237": "瓷砖地板",
        "884238": "血液制品",
        "884239": "疫苗",
        "884240": "其他生物制品",
        "884242": "医疗耗材",
        "884243": "体外诊断",
        "884244": "医疗研发外包",
        "884245": "其他医疗服务",
        "884246": "电能综合服务",
        "884247": "商业地产",
        "884248": "房地产服务Ⅲ",
        "884249": "国有大型银行",
        "884250": "股份制银行",
        "884251": "城商行",
        "884252": "农商行",
        "884253": "其他银行",
        "884254": "旅游零售",
        "884255": "互联网电商Ⅲ",
        "884256": "教育Ⅲ",
        "884257": "专业服务",
        "884258": "体育",
        "884259": "其他社会服务Ⅲ",
        "884260": "游戏",
        "884261": "数字媒体",
        "884262": "通信网络设备及器件",
        "884263": "通信线缆及配套",
        "884264": "通信终端及配件",
        "884265": "其他通信设备",
        "884266": "军工电子",
        "884267": "大气治理",
        "884268": "水务及水治理",
        "884269": "固废治理",
        "884270": "综合环境治理",
        "884271": "个护用品",
        "884272": "化妆品",
        "884273": "医疗美容",
        "884274": "IT服务",
    }

@retry(tries=3, delay=1)
def get_code_id(code):
    """
    生成东方财富股票专用的行情ID
    code:可以是代码或简称或英文
    """
    if code in code_id_dict.keys():
        return code_id_dict[code]
    url = 'https://searchapi.eastmoney.com/api/suggest/get'
    params = (
        ('input', f'{code}'),
        ('type', '14'),
        ('token', 'D43BF722C8E33BDC906FB84D85E326E8'),
    )
    response = session.get(url, params=params).json()
    code_dict = response['QuotationCodeTable']['Data']
    if code_dict:
        return code_dict[0]['QuoteID']
    else:
        print('输入代码有误')

def trans_num(df, ignore_cols):
    '''df为需要转换数据类型的dataframe
    ignore_cols为dataframe中忽略要转换的列名的list
    如ignore_cols=['代码','名称','所处行业']
    '''
    trans_cols = list(set(df.columns) - set(ignore_cols))
    df[trans_cols] = df[trans_cols].apply(lambda s: pd.to_numeric(s, errors='coerce'))
    return df

#同花顺股票池

def get_ths_header():
    file= Path(__file__).parent/"ths.js"
    with open(file) as f:
        js_data = f.read()
    js_code = py_mini_racer.MiniRacer()
    js_code.eval(js_data)
    v_code = js_code.call("v")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36",
        "Cookie": f"v={v_code}",
        }
    return headers
