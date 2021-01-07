# -- coding utf-8 --
import datetime
import json
import logging
import threading

import pika


logger = logging.getLogger()
logger.setLevel(logging.INFO)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logger.addHandler(console)

# ---------------------------- rabbitmq配置 ----------------------------
RABBIT_USER = 'wk'
RABBIT_PWD = 'wk'
RABBIT_HOST = '192.168.101.4'
RABBIT_PORT = 5672
VIRTUAL_HOST = 'wk'

EXCHANGE_TASK_NAME = 'wk.exchange'
EXCHANGE_DATA_NAME = 'wk.exchange'
EXCHANGE_HEARTBEAT_NAME = 'wk.exchange'
EXCHANGE_COMMAND_NAME = 'wk.exchange'
ROUTING_KEY_TASK_NAME = 'wk.routing.task'
ROUTING_KEY_DATA_NAME = 'wk.routing.data'
ROUTING_KEY_HEARTBEAT_NAME = 'wk.routing.heartbeat'
ROUTING_KEY_COMMAND_NAME = 'wk.routing.command'
# QUEUE_TASK_NAME = 'wk.queue.task'
# QUEUE_DATA_NAME = 'wk.queue.data'
QUEUE_TASK_NAME = ROUTING_KEY_TASK_NAME
QUEUE_DATA_NAME = ROUTING_KEY_DATA_NAME
QUEUE_HEARTBEAT_NAME = ROUTING_KEY_HEARTBEAT_NAME
QUEUE_COMMAND_NAME = ROUTING_KEY_COMMAND_NAME


class RabbitQueue(object):
    def __init__(self):
        self.conn = None
        self.channel = self.create_channel()

    def create_channel(self):
        try:
            credentials = pika.PlainCredentials(RABBIT_USER, RABBIT_PWD)
            self.conn = pika.BlockingConnection(
                pika.ConnectionParameters(RABBIT_HOST, RABBIT_PORT, VIRTUAL_HOST, credentials, heartbeat=0))
            channel = self.conn.channel()
        except Exception as e:
            import traceback
            ex_str = traceback.format_exc()
            logger.error(ex_str)
            channel = None
        return channel

    def init_exchange_info(self, queue_name, exchange_name, routing_key, callback, exchange_type='direct'):
        self.create_queue(queue_name)  # 尝试在rabbitmq 建立指定队列，如果指定队列存在不做任何操作。
        self.create_exchange(exchange_name, exchange_type)
        self.bind_queue_by_routing_key(queue_name, exchange_name, routing_key)
        self.listen_msg(queue_name, callback)  # 设置监听消息队列回调

    def create_queue(self, queue_name):
        self.channel.queue_declare(queue=queue_name, durable=True)

    def create_exchange(self, exchange, exchange_type='topic'):
        self.channel.exchange_declare(exchange=exchange, exchange_type=exchange_type, durable=True)

    def bind_queue_by_routing_key(self, queue_name, exchange, routing_key):
        self.channel.queue_bind(queue_name, exchange, routing_key)

    def send_msg(self, msg, routing_key, exchange=''):
        self.channel.basic_publish(exchange=exchange, routing_key=routing_key, body=msg)

    def listen_msg(self, queue_name, callback, auto_ack=False):
        self.channel.basic_consume(queue_name, callback, auto_ack=auto_ack)

    def receive_msg(self):
        self.channel.start_consuming()

    def get_msg(self, queue_name):
        self.channel.basic_get(queue_name)

    def close(self):
        self.conn.close()

    def reconnect(self):
        self.create_channel()


def get_command_rbq():
    command_rbq = RabbitQueue()
    return command_rbq


def hand_out_command(command_rbq, sid):
    """ 下发指令 """
    try:
        command_rbq.send_msg(json.dumps(sid), routing_key=ROUTING_KEY_COMMAND_NAME, exchange=EXCHANGE_COMMAND_NAME)
        logger.info("下发指令 Sent {}".format(json.dumps(sid)))
    except Exception as e:
        import traceback
        ex_str = traceback.format_exc()
        logger.error(ex_str)
        command_rbq = get_command_rbq()
        command_rbq.send_msg(json.dumps(sid), routing_key=ROUTING_KEY_COMMAND_NAME, exchange=EXCHANGE_COMMAND_NAME)
        logger.info("下发指令 Sent {}".format(json.dumps(sid)))


def get_heartbeat_rbq():
    heartbeat_rbq = RabbitQueue()
    # heartbeat_rbq.create_exchange(EXCHANGE_HEARTBEAT_NAME)
    heartbeat_rbq.create_queue(QUEUE_HEARTBEAT_NAME)
    heartbeat_rbq.bind_queue_by_routing_key(QUEUE_HEARTBEAT_NAME, EXCHANGE_HEARTBEAT_NAME, ROUTING_KEY_HEARTBEAT_NAME)
    return heartbeat_rbq


def crawl_engine_heart(heartbeat_rbq, callback):
    try:
        heartbeat_rbq.listen_msg(QUEUE_HEARTBEAT_NAME, callback)
        logger.info(' Waiting for heartbeat ... ')
        heartbeat_rbq.receive_msg()
    except Exception as e:
        import traceback
        ex_str = traceback.format_exc()
        logger.error(ex_str)
        heartbeat_rbq = get_heartbeat_rbq()
        heartbeat_rbq.listen_msg(QUEUE_HEARTBEAT_NAME, callback)
        logger.info(' Waiting for heartbeat ... ')
        heartbeat_rbq.receive_msg()


wf_heartbeat_time = datetime.datetime.now()
wp_heartbeat_time = datetime.datetime.now()


def crawl_engine_heart_callback(channel, method, properties, body):
    """
    {"heartbeat":"WF"}
    """
    global wf_heartbeat_time
    global wp_heartbeat_time
    logger.info("监听到采集引擎心跳..")
    message = json.loads(body)
    channel.basic_ack(delivery_tag=method.delivery_tag)
    logger.info("crawl_engine_heart_callback{}".format(message))
    platform = message['heartbeat']
    if platform == "WF":
        wf_heartbeat_time = datetime.datetime.now()
    if platform == "WP":
        wp_heartbeat_time = datetime.datetime.now()


if __name__ == '__main__':
    command_rbq = get_command_rbq()
    hand_out_command(command_rbq, "restart")
    heartbeat_rbq = get_heartbeat_rbq()
    crawl_engine_t = threading.Thread(target=crawl_engine_heart, name="crawl_engine_heart",
                                      args=(heartbeat_rbq, crawl_engine_heart_callback))
    crawl_engine_t.start()
