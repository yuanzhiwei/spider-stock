#!/usr/bin/python
# coding: utf-8

"""
爬取东方财富人气榜
"""
import os
import time
import datetime
import logging
import logging.handlers
import sys
from PIL import Image

import main
import util.driverutil
from selenium.webdriver.common.by import By

from util.WebDriverLib import WebDriverLib


class popularity_spider:
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
        LOG_FILE = 'popularity_spider_' + time.strftime('%Y-%m-%d') + '.log'
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

        self.top10 = []

    def join_images(self, png1, png2, size=0, output='result.jpg'):
        """
        图片拼接
        :param png1: 图片1
        :param png2: 图片2
        :param size: 两个图片重叠的距离
        :param output: 输出的图片文件
        :return:
        """
        # 图片拼接
        img1, img2 = Image.open(png1), Image.open(png2)
        size1, size2 = img1.size, img2.size  # 获取两张图片的大小
        joint = Image.new('RGB', (size1[0], size1[1] + size2[1] - size))  # 创建一个空白图片
        # 设置两张图片要放置的初始位置
        loc1, loc2 = (0, 0), (0, size1[1] - size)
        # 分别放置图片
        joint.paste(img1, loc1)
        joint.paste(img2, loc2)
        # 保存结果
        joint.save(output)

    def run(self):
        driver = util.driverutil.get_chrome_driver_without_proxy('https://guba.eastmoney.com/rank/', 30, r'D:/software/anaconda/chromedriver.exe', False)
        driver.maximize_window()
        # driver.execute_script('document.body.style.zoom="0.8"')

        driver.execute_script('arguments[0].style.display="none";', driver.find_element(By.ID, 'sharewrap'))
        driver.execute_script('arguments[0].style.display="none";', driver.find_element(By.ID, 'backbox'))

        for tr in driver.find_elements(By.XPATH, '//table[@class="rank_table"]/*/tr'):
            driver.execute_script('arguments[0].style.display="none";', tr.find_elements(By.XPATH, './td')[5])


        element = driver.find_element(By.XPATH, '//table[@class="rank_table"]')
        path = os.path.abspath("./img")

        webdriverlib = WebDriverLib(driver)
        webdriverlib.take_screenshot_on_item(driver, element, path)

        self.join_images(os.path.join('./img', 'screenshot0.png'), os.path.join('./img', 'screenshot1.png'))

        path = os.path.abspath("./")
        img_name = 'result'
        img_path = os.path.abspath(f"{path}\\{img_name}.jpg")
        msg = img_path.replace("\\", "\\\\")
        room_id_list = main.room_id.split(",")
        for i in range(len(room_id_list)):
            main.send_img_room(msg, room_id_list[i])



    def run_old(self):
        driver = util.driverutil.get_chrome_driver_without_proxy('https://guba.eastmoney.com/rank/', 30, r'D:/software/anaconda/chromedriver.exe', False)
        driver.maximize_window()
        driver.execute_script("""
            (function () {
              var y = 0;
              var step = 100;
              window.scroll(0, 0);

              function f() {
                if (y < document.body.scrollHeight) {
                  y += step;
                  window.scroll(0, y);
                  setTimeout(f, 50);
                } else {
                  window.scroll(0, 0);
                  document.title += "scroll-done";
                }
              }

              setTimeout(f, 1000);
            })();
          """)
        for i in range(5):
            if "scroll-done" in driver.title:
                break
            time.sleep(1)

        time.sleep(2)
        # driver.execute_script('document.body.style.zoom="0.8"')
        # div = driver.find_element(By.XPATH, '//div[@class="tablebox"]')
        # div.screenshot('test.png')
        driver.save_screenshot('test.png')
        driver.close()


# main
if __name__ == '__main__':
    '''
    东方财富人气榜
    '''
    print('''
    **********************************
    **           监控spider          **
    **********************************
    ''')
    popularity_spider().run()
