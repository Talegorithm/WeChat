import json
import threading
from common.log import logger
from common.mq_client import MQClient
from channel.chat_message import ChatMessage
from bridge.reply import Reply, ReplyType

class MQListener:
    def __init__(self, channel):
        self.channel = channel
        self.mq_client = MQClient()
        
    def start(self):
        """启动一个新线程监听响应队列"""
        threading.Thread(target=self._listen_responses, daemon=True).start()
        
    def _listen_responses(self):
        """监听响应队列的处理函数"""
        def callback(ch, method, properties, body):
            try:
                logger.info(f"[MQ] Received response from queue: {body}")
                response = json.loads(body)
                self._process_response(response)
            except json.JSONDecodeError as e:
                logger.error(f"[MQ] Invalid JSON format in response: {e}")
            except Exception as e:
                logger.error(f"[MQ] Failed to process message: {e}")
                
        logger.info("[MQ] Starting to consume from response queue...")
        self.mq_client.start_consuming(callback)

    def _process_response(self, response_data):
        """处理从响应队列收到的消息"""
        try:
            logger.debug(f"[MQ] Processing response: {response_data}")
            session_id = response_data.get("session_id")
            content = response_data.get("content")
            
            if not session_id or not content:
                logger.error(f"[MQ] Missing required fields in response: {response_data}")
                return
                
            # 获取之前保存的context
            context = self.channel.session_id_to_context.get(session_id)
            if not context:
                logger.error(f"[MQ] Context not found for session_id: {session_id}")
                return
                
            logger.info(f"[MQ] Found context for session_id {session_id}, preparing to send reply")
            
            # 构造Reply对象
            reply = Reply(ReplyType.TEXT, content)
            
            # 使用原有的装饰和发送逻辑
            reply = self.channel._decorate_reply(context, reply)
            self.channel._send_reply(context, reply)
            
            # 清理context
            del self.channel.session_id_to_context[session_id]
            logger.info(f"[MQ] Successfully processed and sent reply for session_id: {session_id}")
            
        except Exception as e:
            logger.error(f"[MQ] Failed to process response: {e}")