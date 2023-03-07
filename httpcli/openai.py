"""
@Project ：WechatBot 
@File    ：openai.py
@IDE     ：PyCharm 
@Author  ：yuanzhiwei
@Date    ：2023/2/17 14:03
"""
import configparser
import os

import openai

from httpcli.output import *

current_path = os.path.dirname(__file__)
config_path = os.path.join(current_path, "../config/config.ini")
config = configparser.ConfigParser()  # 类实例化
config.read(config_path, encoding="utf-8")
openai_key = config.get("apiService", "openai_key")
openai.api_key = openai_key


def OpenaiServer(msg=None):
    try:
        if msg is None:
            output(f'ERROR：msg is None')
            msg = ""
        else:
            response = openai.Completion.create(
                model="gpt-3.5-turbo",
                prompt=msg,
                temperature=0.9,
                max_tokens=2048,
                top_p=1.0,
                frequency_penalty=0.0,
                presence_penalty=0
            )
            msg = response.choices[0].text.strip()
    except Exception as e:
        output(f"ERROR：{e}")
        msg = e.message
    return msg
