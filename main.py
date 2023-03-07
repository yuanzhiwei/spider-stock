"""
@Project ：WechatBot
@File    ：openai.py
@IDE     ：PyCharm
@Author  ：yuanzhiwei
@Date    ：2023/2/15 19:12
"""
from httpcli.output import *
from servercli.server import *
import configparser
import schedule
from multiprocessing import Process
import datetime
from pyfiglet import Figlet
import pymysql

# 读取本地的配置文件

current_path = os.path.dirname(__file__)
config_path = os.path.join(current_path, "./config/config.ini")
config = configparser.ConfigParser()  # 类实例化
config.read(config_path, encoding="utf-8")
admin_id = config.get("server", "admin_id")
room_id = config.get("server", "room_id")
set_time_am = config.get("server", "set_time_am")
set_time_pm = config.get("server", "set_time_pm")
after_work_time = config.get("server", "after_work_time")


pymysql.install_as_MySQLdb()


conn = pymysql.connect(
    host=config.get('local', 'local_host'),
    port=3306,
    user=config.get('local', 'local_username'),
    passwd=config.get('local', 'local_password'),
    db=config.get('local', 'local_db'),
    charset='utf8')

conn.ping(True)

cur = conn.cursor()


def morning_paper_push():
    output("每日早报推送")
    msg = get_history_event()
    room_id_list = room_id.split(",")
    for i in range(len(room_id_list)):
        send_img_room(msg, room_id_list[i])

def everyday_after_work_push():
    output("下班通知推送")
    if (
        int(datetime.date.today().isoweekday()) == 6
        or int(datetime.date.today().isoweekday()) == 7
    ):
        msg = ""
    else:
        msg = "各部门请注意，下班时间已到！！！不要浪费电费，请火速回家！\n[Doge] over"
    room_id_list = room_id.split(",")
    for i in range(len(room_id_list)):
        auto_send_message_room(msg, room_id_list[i])

def ths_news_push(kafka_op):
    # sql = "select * from monitor_news where pushed = false order by time limit 5"
    # cur.execute(sql)
    # results = cur.fetchall()
    # if len(results) == 0:
    #     print("持续监控中...")
    #     time.sleep(40)
    # else:
    #     print('开始推送')
    # for row in results:
    #     content = '''【%s】
    # %s
    # %s
    # %s
    # 发布时间:%s''' % (row[5], row[1], row[4], row[2], row[3])
    #     output("同花顺监控推送")
    #     room_id_list = room_id.split(",")
    #     for i in range(len(room_id_list)):
    #         auto_send_message_room(content, room_id_list[i])
    #     cur.execute('update monitor_news set pushed = 1, pushed_time=now() where id = %d' % row[0])
    #     conn.commit()
    pass

# 创建定时任务
def auto_push():

    output("每日定时任务 Strat")
    try:
        # 早报自动推送
        schedule.every().day.at(set_time_am).do(morning_paper_push)
        # 晚报自动推送
        # schedule.every().day.at(set_time_pm).do(evening_paper_push)
        # 下班通知推送
        schedule.every().day.at(after_work_time).do(everyday_after_work_push)
    except Exception as r:
        output(str(r))

    while True:
        schedule.run_pending()
        # ths_news_push(kafka_op)

def main():
    output("wx消息推送 Run ....")
    get_personal_info()
    processed = Process(target=auto_push, name="Auto push")
    # 进程守护
    processed.daemon = True
    processed.start()
    bot()


if __name__ == "__main__":
    f = Figlet(font="slant", width=2000)
    cprint(f.renderText("wx消息推送"), "green")
    main()
