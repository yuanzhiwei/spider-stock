# -*- coding: utf-8 -*-


import time
import json
import ujson
import random
from kafka import KafkaProducer, KafkaConsumer
import logging
import logging.handlers

import main


class KafkaOperate(object):

    def __init__(self, bootstrap_servers=None):

        # logging
        LOG_FILE = time.strftime('%Y-%m-%d') + '-kafka.log'
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

        if not bootstrap_servers:
            raise Exception('bootstrap_servers is None')

        self.__bootstrap_servers = None
        if isinstance(bootstrap_servers, str):
            ip_port_string = bootstrap_servers.strip()
            if ',' in ip_port_string:
                self.__bootstrap_servers = ip_port_string.replace(' ', '').split(',')
            else:
                self.__bootstrap_servers = [ip_port_string]

        self.kafka_consumer = None

        pass

    def __del__(self):
        pass

    def kfk_consume(self, *topic_name, group_id='my_group'):
        if not self.kafka_consumer:
            self.kafka_consumer = KafkaConsumer(
                *topic_name, group_id=group_id,
                bootstrap_servers=self.__bootstrap_servers,
                auto_offset_reset='earliest',
            )
        for msg in self.kafka_consumer:
            if msg.topic == 'new_concepts':
                self.logger.info('new_concepts: {0}'.format(msg))
                value = json.loads(msg.value)
                content = '''
%s
%s
%s
发布时间:%s''' % (value['title'], value['description'], value['url'], value['time'])
                self.logger.info("同花顺监控推送")
                room_id_list = main.room_id.split(",")
                for i in range(len(room_id_list)):
                    main.auto_send_message_room(content, room_id_list[i])

            if msg.topic == 'dfcf_stock_change':
                self.logger.info('new_concepts: {0}'.format(msg))
                value = json.loads(msg.value)
                content = '''
%s
%s
%s''' % (value['标题'], value['异动时间'], value['主力净流入'], value['成交量'])
                self.logger.info("东方财富异动")
                room_id_list = main.room_id.split(",")
                for i in range(len(room_id_list)):
                    main.auto_send_message_room(content, room_id_list[i])

    def __kfk_produce(self, topic_name=None, data_dict=None, partition=None):
        """
            如果想要多线程进行消费，可以设置 发往不通的 partition
            有多少个 partition 就可以启多少个线程同时进行消费，
        :param topic_name:
        :param data_dict:
        :param partition:
        :return:
        """
        if not self.kafka_producer:
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=self.__bootstrap_servers,
                client_id='my_group',
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
        # data_dict = {
        #     "name": 'king',
        #     'age': 100,
        #     "msg": "Hello World"
        # }
        if partition:
            self.kafka_producer.send(
                topic=topic_name,
                value=data_dict,
                # key='count_num',  # 同一个key值，会被送至同一个分区
                partition=partition
            )
        else:
            self.kafka_producer.send(topic_name, data_dict)
        pass

    @staticmethod
    def get_consumer(group_id: str, bootstrap_servers: list, topic: str, enable_auto_commit=True) -> KafkaConsumer:
        topics = tuple([x.strip() for x in topic.split(',') if x.strip()])
        if enable_auto_commit:
            return KafkaConsumer(
                *topics,
                group_id=group_id,
                bootstrap_servers=bootstrap_servers,
                auto_offset_reset='earliest',
                # fetch_max_bytes=FETCH_MAX_BYTES,
                # connections_max_idle_ms=CONNECTIONS_MAX_IDLE_MS,
                # max_poll_interval_ms=KAFKA_MAX_POLL_INTERVAL_MS,
                # session_timeout_ms=SESSION_TIMEOUT_MS,
                # max_poll_records=KAFKA_MAX_POLL_RECORDS,
                # request_timeout_ms=REQUEST_TIMEOUT_MS,
                # auto_commit_interval_ms=AUTO_COMMIT_INTERVAL_MS,
                value_deserializer=lambda m: ujson.loads(m.decode('utf-8'))
            )
        else:
            return KafkaConsumer(
                *topics,
                group_id=group_id,
                bootstrap_servers=bootstrap_servers,
                auto_offset_reset='earliest',
                # fetch_max_bytes=FETCH_MAX_BYTES,
                # connections_max_idle_ms=CONNECTIONS_MAX_IDLE_MS,
                # max_poll_interval_ms=KAFKA_MAX_POLL_INTERVAL_MS,
                # session_timeout_ms=SESSION_TIMEOUT_MS,
                # max_poll_records=KAFKA_MAX_POLL_RECORDS,
                # request_timeout_ms=REQUEST_TIMEOUT_MS,
                enable_auto_commit=enable_auto_commit,
                value_deserializer=lambda m: ujson.loads(m.decode('utf-8'))
            )


if __name__ == '__main__':
    bs = 'localhost:9092'
    kafka_op = KafkaOperate(bootstrap_servers=bs)
    kafka_op.kfk_consume('new_concepts')
    kafka_op.kfk_consume('dfcf_stock_change')
    pass
