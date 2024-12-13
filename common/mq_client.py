import pika
import json
from config import conf
from common.log import logger
import time

class MQClient:
    def __init__(self):
        self.host = conf().get("mq_host", "rabbitmq")
        self.request_queue = conf().get("mq_request_queue", "wechat_request")
        self.response_queue = conf().get("mq_response_queue", "wechat_response")
        self.connection = None
        self.channel = None
        
        # 初始化连接
        self._establish_connection()
        
    def _establish_connection(self):
        """建立连接并创建channel"""
        try:
            # 清理旧连接
            if self.channel:
                try:
                    self.channel.close()
                except:
                    pass
            if self.connection:
                try:
                    self.connection.close()
                except:
                    pass
            
            # 建立新连接
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(
                host=self.host,
                heartbeat=600,
                blocked_connection_timeout=300,
                credentials=pika.PlainCredentials('guest', 'guest')  # 默认凭据
            ))
            self.channel = self.connection.channel()
            
            # 声明队列
            self.channel.queue_declare(
                queue=self.request_queue,
                durable=True,
                auto_delete=False,
                exclusive=False
            )
            self.channel.queue_declare(
                queue=self.response_queue,
                durable=True,
                auto_delete=False,
                exclusive=False
            )
            
            logger.info("[MQ] Successfully established connection and declared queues")
            return True
            
        except Exception as e:
            logger.error(f"[MQ] Failed to establish connection: {e}")
            if self.channel:
                try:
                    self.channel.close()
                except:
                    pass
            if self.connection:
                try:
                    self.connection.close()
                except:
                    pass
            self.connection = None
            self.channel = None
            return False

    def send_to_request_queue(self, message, retry_times=None):
        """发送消息到请求队列，支持重试"""
        logger.debug(f"[MQ] Attempting to send message: {json.dumps(message)}")
        
        if not self.channel or self.channel.is_closed:
            self._establish_connection()
            
        if not self.channel:
            logger.error("[MQ] No valid channel available")
            return False
            
        try:
            self.channel.basic_publish(
                exchange='',
                routing_key=self.request_queue,
                body=json.dumps(message),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # 使消息持久化
                    content_type='application/json'
                )
            )
            logger.info(f"[MQ] Message successfully published to {self.request_queue}")
            return True
        except Exception as e:
            logger.error(f"[MQ] Failed to send message: {e}")
            self._establish_connection()  # 尝试重新建立连接
            return False

    def start_consuming(self, callback):
        """启动消费响应队列"""
        while True:
            try:
                if not self.channel or self.channel.is_closed:
                    self._establish_connection()
                    
                if not self.channel:
                    logger.error("[MQ] No valid channel available, retrying in 5 seconds...")
                    time.sleep(5)
                    continue
                    
                logger.info(f"[MQ] Successfully connected to response queue: {self.response_queue}")
                
                self.channel.basic_consume(
                    queue=self.response_queue,
                    on_message_callback=callback,
                    auto_ack=True
                )
                
                logger.info("[MQ] Starting to consume messages...")
                self.channel.start_consuming()
                
            except pika.exceptions.AMQPConnectionError as e:
                logger.error(f"[MQ] Connection error: {e}, reconnecting...")
                time.sleep(5)
            except Exception as e:
                logger.error(f"[MQ] Unexpected error while consuming: {e}")
                time.sleep(5)
            finally:
                if self.connection and not self.connection.is_closed:
                    self.connection.close()

    def check_queue_status(self):
        """检查队列状态"""
        try:
            if not self.channel or self.channel.is_closed:
                self._establish_connection()
            
            if not self.channel:
                return
            
            # 获取请求队列状态
            req_status = self.channel.queue_declare(queue=self.request_queue, passive=True)
            logger.info(f"[MQ] Request queue status - Messages: {req_status.method.message_count}, Consumers: {req_status.method.consumer_count}")
            
            # 获取响应队列状态
            resp_status = self.channel.queue_declare(queue=self.response_queue, passive=True)
            logger.info(f"[MQ] Response queue status - Messages: {resp_status.method.message_count}, Consumers: {resp_status.method.consumer_count}")
            
        except Exception as e:
            logger.error(f"[MQ] Failed to check queue status: {e}")