# -*- coding: utf-8 -*-


import time
import json
import ujson
import random
from kafka import KafkaProducer, KafkaConsumer
import logging
import logging.handlers


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

        self.kafka_producer = None
        self.kafka_consumer = None

        pass

    def __del__(self):
        pass

    def kfk_consume(self, topic_name=None, group_id='my_group'):
        if not self.kafka_consumer:
            self.kafka_consumer = KafkaConsumer(
                topic_name, group_id=group_id,
                bootstrap_servers=self.__bootstrap_servers,
                auto_offset_reset='earliest',
            )
        count = 0
        for msg in self.kafka_consumer:
            count += 1
            # message value and key are raw bytes -- decode if necessary!
            # e.g., for unicode: `message.value.decode('utf-8')`
            info = f'[{count}] {msg.topic}:{msg.partition}:{msg.offset}: key={msg.key}, value={msg.value.decode("utf-8")}'
            self.logger.info(info)
            time.sleep(1)

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

    def kfk_produce_one(self, topic_name=None, data_dict=None, partition=None, partition_count=1):
        partition = partition if partition else random.randint(0, partition_count - 1)
        self.__kfk_produce(topic_name=topic_name, data_dict=data_dict, partition=partition)
        self.kafka_producer.flush()

    def kfk_produce_many(self, topic_name=None, data_dict_list=None, partition=None, partition_count=1, per_count=100):
        count = 0
        for data_dict in data_dict_list:
            partition = partition if partition else count % partition_count
            self.__kfk_produce(topic_name=topic_name, data_dict=data_dict, partition=partition)
            if 0 == count % per_count:
                self.kafka_producer.flush()
            count += 1
        self.kafka_producer.flush()
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

    @staticmethod
    def get_producer(bootstrap_servers: list):
        return KafkaProducer(bootstrap_servers=bootstrap_servers, retries=5)


if __name__ == '__main__':
    bs = 'localhost:9092'
    kafka_op = KafkaOperate(bootstrap_servers=bs)
    kafka_op.kfk_consume(topic_name='001_test')
    pass