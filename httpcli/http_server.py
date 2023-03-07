"""
@Project ：WechatBot
@File    ：openai.py
@IDE     ：PyCharm
@Author  ：yuanzhiwei
@Date    ：2023/2/16 12:33
"""
import configparser
import os
import re

import requests

from httpcli.output import *

# 读取本地的配置文件
current_path = os.path.dirname(__file__)
config_path = os.path.join(current_path, "../config/config.ini")
config = configparser.ConfigParser()  # 类实例化
config.read(config_path, encoding="utf-8")
history_url = config.get("apiService", "history_url")
girl_videos_url = config.get("apiService", "girl_videos_url")
ai_reply_url = config.get("apiService", "ai_reply_url")


# 每日早報
def get_history_event():
    output("Get History event")
    try:
        resp = requests.get(
            history_url,
            timeout=5,
            verify=False,
        )
        if resp.status_code == 200:
            path = os.path.abspath("./img")
            img_name = int(time.time() * 1000)
            # 以时间轴的形式给图片命名
            with open(f"{path}\\{img_name}.jpg", "wb+") as f:
                f.write(resp.content)
                f.close()
            video_path = os.path.abspath(f"{path}\\{img_name}.jpg")
            msg = video_path.replace("\\", "\\\\")
        else:
            msg = "每日早報接口调用超时"
    except Exception as e:
        msg = "每日早報接口调用出错，错误信息：{}".format(e)
    return msg

# 获取美女视频接口
def get_girl_videos():
    output("Get Girl Videos")
    try:
        resp = requests.get(girl_videos_url, timeout=5, verify=False)
        if resp.status_code == 200:
            videos_url = re.findall(
                '<video src="(.*?)" muted controls preload="auto"', resp.text, re.S
            )
            if len(videos_url) > 0:
                url = "http:" + str(videos_url[0])
                resp1 = requests.get(url, timeout=5, verify=False)
                path = os.path.abspath("./video")
                videos_name = int(time.time() * 1000)
                # 以时间轴的形式给图片命名
                with open(f"{path}\\{videos_name}.mp4", "wb+") as f:
                    # 写入文件夹
                    f.write(resp1.content)  # 如果这句话一直报错，很有可能是你的网址url不对
                    # 关闭文件夹
                    f.close()
                video_path = os.path.abspath(f"{path}\\{videos_name}.mp4")
                msg = video_path.replace("\\", "\\\\")
            else:
                msg = "ERROR：未识别到URL连接"
                output(msg)
        else:
            msg = "站点状态异常，访问请求：{}".format(resp.status_code)
    except Exception as e:
        output("ERROR：{}".format(e))
        msg = "视频接口调用出错，错误信息：{}".format(e)
    return msg


# AI闲聊接口信息
def ai_reply(self):
    output("GET AI Reply")
    try:
        resp = requests.get(str(ai_reply_url) + str(self), timeout=5, verify=False)
        if resp.status_code == 200 and resp.json()["result"] == 0:
            msg = resp.json()["content"]
        else:
            msg = "你消息发送的太频繁了，慢一点"
    except Exception as e:
        output(f"ERROR：{e}")
        msg = f"AI对话机器人接口调用出错，ERROR：{e}"
    return msg


# 计算时间差函数
def diff_day(start_day, end_day):
    start_sec = time.mktime(time.strptime(start_day, "%Y-%m-%d"))
    end_sec = time.mktime(time.strptime(end_day, "%Y-%m-%d"))
    return int((end_sec - start_sec) / 86400)


def diff_hour(start_hour, end_hour):
    start_sec = time.mktime(time.strptime(start_hour, "%Y-%m-%d %H:%M:%S"))
    end_sec = time.mktime(time.strptime(end_hour, "%Y-%m-%d %H:%M:%S"))
    return [
        int((end_sec - start_sec) / 3600),
        int((end_sec - start_sec) / 60) - int((end_sec - start_sec) / 3600) * 60,
    ]


