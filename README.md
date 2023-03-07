## 部署教程

安装微信**3.2.1.121**版本，然后再打开ServerMain/server中的微信DLL注入器V1.0.3.exe工具进行dll注入

![img.png](images/img.png) 

点击注入即可，然后接下来就是项目的config目录下面的config.ini的配置

***注意编辑config文件不要用txt打开会改变文件的类型，导致报错！！！**

config.ini详解

```
[local]
local_db=ths
local_host=localhost
local_username=root
local_password=123456
[server]
# 微信服务端的配置文件
ip = 127.0.0.1
port = 5555
# 管理员wxid
admin_id = wxid_2979419802612
# 推送的微信群聊地址
room_id = 34400566498@chatroom
# 视频权限群聊地址
video_list_room_id = 34400566498@chatroom,7354342599@chatroom
# 早报自动推送时间
set_time_am = 08:30
# 股市休市时间
after_work_time = 15:00
set_time_pm = 20:00
[apiService]
# openai用户key
openai_key = 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
# 60s读懂世界
history_url = https://api.qqsuu.cn/api/dm-60s
# 美女视频接口
girl_videos_url = https://tucdn.wpon.cn/api-girl/
# ai对话机器人接口
ai_reply_url = http://api.qingyunke.com/api.php?key=free&msg=
```

```bash
pip3 install -r requirements.txt
```

如果出现WebSocketApp no modle的报错，使用命令安装此依赖即可

```bash
pip3 install websocket-client-py3
```

然后使用命令即可启动项目

```bash
python3 main.py
```

***建议使用python3.8以上**
