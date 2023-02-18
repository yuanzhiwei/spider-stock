#!/usr/bin/python
# coding: utf-8
import time
import logging
import logging.handlers
import sys
import util.mysql
# from wxpy import *
import itchat


class wxbot:
    def __init__(self):
        LOG_FILE = time.strftime('%Y-%m-%d') + '-wx.log'
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
        # 初始化机器人，扫码登陆
        # self.bot = Bot()
        itchat.auto_login(hotReload=True)

    def run(self):
        # my_friend = self.bot.friends().search('远方的诗', sex=MALE, city="杭州")[0]
        # # 发送文本给好友
        # my_friend.send('Hello WeChat!')
        # author = itchat.search_friends(nickName='远方的诗')[0]
        # author.send('test!')
        chatroomName = '监控测试'
        itchat.get_chatrooms(update=True)
        chatrooms = itchat.search_chatrooms(name=chatroomName)
        if chatrooms is None:
            print(u'没有找到群聊：' + chatroomName)
        else:
            chatroom = itchat.update_chatroom(chatrooms[0]['UserName'])
            while True:
                sql = "select * from monitor_news where pushed = 0 order by time limit 5"
                util.mysql.cur.execute(sql)
                results = util.mysql.cur.fetchall()
                if len(results) == 0:
                    print("持续监控中...")
                    time.sleep(60)
                else:
                    print('开始推送')
                for row in results:
                    content = '''【%s】
%s
%s
%s
发布时间:%s''' % (row[5], row[1], row[4], row[2], row[3])
                    chatroom.send(content)
                    util.mysql.cur.execute('update monitor_news set pushed = 1, pushed_time=now() where id = %d' % row[0])
                    util.mysql.conn.commit()

# main
if __name__ == '__main__':
    wxbot().run()