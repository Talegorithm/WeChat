import os
import re
import threading
import time
from asyncio import CancelledError
from concurrent.futures import Future, ThreadPoolExecutor

from bridge.context import *
from bridge.reply import *
from channel.channel import Channel
from common.dequeue import Dequeue
from common import memory
from common.mq_client import MQClient
from common.mq_listener import MQListener
from plugins import *

try:
    from voice.audio_convert import any_to_wav
except Exception as e:
    pass

handler_pool = ThreadPoolExecutor(max_workers=8)  # 处理消息的线程池


# 抽象类, 它包含了与消息通道无关的通用处理逻辑
class ChatChannel(Channel):
    name = None  # 登录的用户名
    user_id = None  # 登录的用户id
    futures = {}  # 记录每个session_id提交到线程池的future对象, 用于重置会话时把没执行的future取消掉，正在执行的不会被取消
    sessions = {}  # 用于控制并发，每个session_id同时只能有一个context在处理
    lock = threading.Lock()  # 用于控制对sessions的访问
    session_timestamps = {}  # 用于记录会话时间戳

    def __init__(self):
        logger.debug("[chat_channel] Initializing ChatChannel")
        try:
            super().__init__()
            self.mq_client = MQClient()
            if not self.mq_client.channel:
                raise Exception("Failed to establish MQ connection")
            
            # 检查队列状态
            self.mq_client.check_queue_status()
            
            self.session_id_to_context = {}
            self.listener = MQListener(self)
            logger.info("[MQ] Initializing message queue listener...")
            self.listener.start()
            
            # 启动消费者线程
            _thread = threading.Thread(target=self.consume)
            _thread.setDaemon(True)
            _thread.start()
            
            self._init_session_timestamps()
            
        except Exception as e:
            logger.error(f"[chat_channel] Error in initialization: {e}")
            raise

    # 根据消息构造context，消息内容相关的触发项写在这里
    def _compose_context(self, ctype: ContextType, content, **kwargs):
        logger.debug(f"[chat_channel] Composing context: type={ctype}, content={content}, kwargs={kwargs}")
        context = Context(ctype, content)
        context.kwargs = kwargs
        # context首次传入时，origin_ctype是None,
        # 引入的起因是：当输入语音时，会嵌套生成两个context，第一步语音转文本，第二步通过文本生成文字回复。
        # origin_ctype用于第二步文本回复时，判断是否需要匹配前缀，如果是私聊的语音，就不需要匹配前缀
        if "origin_ctype" not in context:
            context["origin_ctype"] = ctype
        # context首次传入时，receiver是None，根据类型设置receiver
        first_in = "receiver" not in context
        # 群名匹配过程，设置session_id和receiver
        if first_in:  # context首次传入时，receiver是None，根据类型设置receiver
            config = conf()
            cmsg = context["msg"]
            user_data = conf().get_user_data(cmsg.from_user_id)
            context["openai_api_key"] = user_data.get("openai_api_key")
            context["gpt_model"] = user_data.get("gpt_model")
            if context.get("isgroup", False):
                group_name = cmsg.other_user_nickname
                group_id = cmsg.other_user_id

                group_name_white_list = config.get("group_name_white_list", [])
                group_name_keyword_white_list = config.get("group_name_keyword_white_list", [])
                if any(
                    [
                        group_name in group_name_white_list,
                        "ALL_GROUP" in group_name_white_list,
                        check_contain(group_name, group_name_keyword_white_list),
                    ]
                ):
                    group_chat_in_one_session = conf().get("group_chat_in_one_session", [])
                    session_id = cmsg.actual_user_id
                    if any(
                        [
                            group_name in group_chat_in_one_session,
                            "ALL_GROUP" in group_chat_in_one_session,
                        ]
                    ):
                        session_id = group_id
                else:
                    logger.debug(f"No need reply, groupName not in whitelist, group_name={group_name}")
                    return None
                context["session_id"] = session_id
                context["receiver"] = group_id
            else:
                context["session_id"] = cmsg.other_user_id
                context["receiver"] = cmsg.other_user_id
            e_context = PluginManager().emit_event(EventContext(Event.ON_RECEIVE_MESSAGE, {"channel": self, "context": context}))
            context = e_context["context"]
            if e_context.is_pass() or context is None:
                return context
            if cmsg.from_user_id == self.user_id and not config.get("trigger_by_self", True):
                logger.debug("[chat_channel]self message skipped")
                return None

        # 消息内容匹配过程，并处理content
        if ctype == ContextType.TEXT:
            if first_in and "」\n- - - - - - -" in content:  # 初次匹配 过滤引用消息
                logger.debug(content)
                logger.debug("[chat_channel]reference query skipped")
                return None

            nick_name_black_list = conf().get("nick_name_black_list", [])
            if context.get("isgroup", False):  # 群聊
                # 校验关键字
                match_prefix = check_prefix(content, conf().get("group_chat_prefix"))
                match_contain = check_contain(content, conf().get("group_chat_keyword"))
                flag = False
                if context["msg"].to_user_id != context["msg"].actual_user_id:
                    if match_prefix is not None or match_contain is not None:
                        flag = True
                        if match_prefix:
                            content = content.replace(match_prefix, "", 1).strip()
                    if context["msg"].is_at:
                        nick_name = context["msg"].actual_user_nickname
                        if nick_name and nick_name in nick_name_black_list:
                            # 黑名单过滤
                            logger.warning(f"[chat_channel] Nickname {nick_name} in In BlackList, ignore")
                            return None

                        logger.info("[chat_channel]receive group at")
                        if not conf().get("group_at_off", False):
                            flag = True
                        self.name = self.name if self.name is not None else ""  # 部分渠道self.name可能没有赋值
                        pattern = f"@{re.escape(self.name)}(\u2005|\u0020)"
                        subtract_res = re.sub(pattern, r"", content)
                        if isinstance(context["msg"].at_list, list):
                            for at in context["msg"].at_list:
                                pattern = f"@{re.escape(at)}(\u2005|\u0020)"
                                subtract_res = re.sub(pattern, r"", subtract_res)
                        if subtract_res == content and context["msg"].self_display_name:
                            # 前缀移除后没有变化，使用群昵称再次移除
                            pattern = f"@{re.escape(context['msg'].self_display_name)}(\u2005|\u0020)"
                            subtract_res = re.sub(pattern, r"", content)
                        content = subtract_res
                if not flag:
                    if context["origin_ctype"] == ContextType.VOICE:
                        logger.info("[chat_channel]receive group voice, but checkprefix didn't match")
                    return None
            else:  # 单聊
                nick_name = context["msg"].from_user_nickname
                if nick_name and nick_name in nick_name_black_list:
                    # 黑名单过滤
                    logger.warning(f"[chat_channel] Nickname '{nick_name}' in In BlackList, ignore")
                    return None

                match_prefix = check_prefix(content, conf().get("single_chat_prefix", [""]))
                if match_prefix is not None:  # 判断如果匹配到自定义前缀，则返回过滤掉前缀+空格后的内容
                    content = content.replace(match_prefix, "", 1).strip()
                elif context["origin_ctype"] == ContextType.VOICE:  # 如果源消息是私聊的语音消息，允许不匹配前缀，放宽条件
                    pass
                else:
                    logger.debug("[chat_channel] No prefix match, message ignored")
                    return None
            content = content.strip()
            img_match_prefix = check_prefix(content, conf().get("image_create_prefix",[""]))
            if img_match_prefix:
                content = content.replace(img_match_prefix, "", 1)
                context.type = ContextType.IMAGE_CREATE
            else:
                context.type = ContextType.TEXT
            context.content = content.strip()
            if "desire_rtype" not in context and conf().get("always_reply_voice") and ReplyType.VOICE not in self.NOT_SUPPORT_REPLYTYPE:
                context["desire_rtype"] = ReplyType.VOICE
        elif context.type == ContextType.VOICE:
            if "desire_rtype" not in context and conf().get("voice_reply_voice") and ReplyType.VOICE not in self.NOT_SUPPORT_REPLYTYPE:
                context["desire_rtype"] = ReplyType.VOICE
        elif ctype == ContextType.URL:
            # URL类型不需要前缀匹配
            if "desire_rtype" not in context:
                context["desire_rtype"] = ReplyType.TEXT
            # 保持原始URL内容
            context.content = content.strip()
        return context

    def _handle(self, context: Context):
        """
        处理消息的主要方法；produce继承自ChatChannel，所以这里需要重写
        原流程: 接收消息 -> 生成回复 -> 装饰回复 -> 发送回复
        新流程: 接收消息 -> 发送到请求队列 -> (异步)等待响应队列 -> 收到回复后发送
        """
        if context is None or not context.content:
            return
        
        session_id = context["session_id"]
        logger.debug("[MQ] Entering _handle with session_id={}, content={}".format(
            session_id, context.content))

        # 保存context,用于后续发送响应时使用
        self.session_id_to_context[session_id] = context
        
        # 构造要发送到队列的消息
        message = {
            "session_id": session_id,
            "content": context.content,
            "type": context.type.name,
            # 可以添加更多需要的字段
            "isgroup": context.kwargs.get("isgroup", False),
            "msg": {
                "from_user_id": context.kwargs.get("msg").from_user_id if context.kwargs.get("msg") else None,
                "from_user_nickname": context.kwargs.get("msg").from_user_nickname if context.kwargs.get("msg") else None,
            }
        }
        
        # 发送到请求队列
        logger.debug(f"[MQ] Message structure before sending: {message}")
        send_result = self.mq_client.send_to_request_queue(message)
        logger.debug(f"[MQ] Send result: {send_result}")
        
        if not send_result:
            # 发送失败的处理
            error_reply = Reply(ReplyType.ERROR, "抱歉，消息处理出现错误，请重试")
            self._send_reply(context, error_reply)
            self._fail_callback(session_id, "Failed to send message to queue", context)
            return
        
        logger.info(f"[MQ] Message sent to request queue, session_id: {session_id}")

        # 注意:这里不再等待回复
        # 回复将由 MQListener 异步处理

    def _decorate_reply(self, context: Context, reply: Reply) -> Reply:
        if reply and reply.type:
            e_context = PluginManager().emit_event(
                EventContext(
                    Event.ON_DECORATE_REPLY,
                    {"channel": self, "context": context, "reply": reply},
                )
            )
            reply = e_context["reply"]
            desire_rtype = context.get("desire_rtype")
            if not e_context.is_pass() and reply and reply.type:
                if reply.type in self.NOT_SUPPORT_REPLYTYPE:
                    logger.error("[chat_channel]reply type not support: " + str(reply.type))
                    reply.type = ReplyType.ERROR
                    reply.content = "不支持发送的消息类型: " + str(reply.type)

                if reply.type == ReplyType.TEXT:
                    reply_text = reply.content
                    if desire_rtype == ReplyType.VOICE and ReplyType.VOICE not in self.NOT_SUPPORT_REPLYTYPE:
                        reply = super().build_text_to_voice(reply.content)
                        return self._decorate_reply(context, reply)
                    if context.get("isgroup", False):
                        if not context.get("no_need_at", False):
                            reply_text = "@" + context["msg"].actual_user_nickname + "\n" + reply_text.strip()
                        reply_text = conf().get("group_chat_reply_prefix", "") + reply_text + conf().get("group_chat_reply_suffix", "")
                    else:
                        reply_text = conf().get("single_chat_reply_prefix", "") + reply_text + conf().get("single_chat_reply_suffix", "")
                    reply.content = reply_text
                elif reply.type == ReplyType.ERROR or reply.type == ReplyType.INFO:
                    reply.content = "[" + str(reply.type) + "]\n" + reply.content
                elif reply.type == ReplyType.IMAGE_URL or reply.type == ReplyType.VOICE or reply.type == ReplyType.IMAGE or reply.type == ReplyType.FILE or reply.type == ReplyType.VIDEO or reply.type == ReplyType.VIDEO_URL:
                    pass
                else:
                    logger.error("[chat_channel] unknown reply type: {}".format(reply.type))
                    return
            if desire_rtype and desire_rtype != reply.type and reply.type not in [ReplyType.ERROR, ReplyType.INFO]:
                logger.warning("[chat_channel] desire_rtype: {}, but reply type: {}".format(context.get("desire_rtype"), reply.type))
            return reply

    def _send_reply(self, context: Context, reply: Reply):
        if reply and reply.type:
            e_context = PluginManager().emit_event(
                EventContext(
                    Event.ON_SEND_REPLY,
                    {"channel": self, "context": context, "reply": reply},
                )
            )
            reply = e_context["reply"]
            if not e_context.is_pass() and reply and reply.type:
                logger.debug("[chat_channel] ready to send reply: {}, context: {}".format(reply, context))
                self._send(reply, context)

    def _send(self, reply: Reply, context: Context, retry_cnt=0):
        try:
            # 添加发送前的日志
            logger.debug(f"[chat_channel] Attempting to send message for session {context['session_id']}")
            
            send_result = self.send(reply, context)
            
            # 如果是错误回复本身，不要再次触发错误处理
            if not send_result and reply.type != ReplyType.ERROR:
                logger.error(f"Send failed for session {context['session_id']}")
                self.reset_session(context['session_id'])
                error_reply = Reply(ReplyType.ERROR, "抱歉，消息处理出现错误，请重试")
                self.send(error_reply, context)
                
            # 添加发送后的确认日志
            logger.debug(f"[chat_channel] Message sent for session {context['session_id']}, result: {send_result}")
            
            return send_result
            
        except Exception as e:
            logger.exception(f"Error in message processing: {e}")
            if reply.type != ReplyType.ERROR:  # 避免错误处理的递归
                self.reset_session(context['session_id'])
                # 尝试重新建立连接
                self.auto_reconnect()
            return False

    def _success_callback(self, session_id, **kwargs):  # 线程正常结束时的回调函数
        logger.debug("Worker return success, session_id = {}".format(session_id))

    def _fail_callback(self, session_id, exception, **kwargs):  # 线程异常结束时的回调函数
        logger.exception("Worker return exception: {}".format(exception))

    def _thread_pool_callback(self, session_id, **kwargs):
        def callback(future):
            try:
                e = future.exception()
                if e is None:
                    self._success_callback(session_id, **kwargs)
                else:
                    self._fail_callback(session_id, e, **kwargs)
            except Exception as e:
                logger.error("[chat_channel] {} exception: {}".format(session_id, e))
            finally:
                with self.lock:
                    if session_id in self.sessions:
                        context_queue, semaphore = self.sessions[session_id]
                        semaphore.release()
        return callback

    def produce(self, context: Context):
        """
        处理接收到的消息
        """
        session_id = context["session_id"]
        logger.debug(f"[chat_channel] produce: session_id={session_id}")
        
        with self.lock:
            if session_id not in self.sessions:
                logger.debug(f"[chat_channel] Creating new session for {session_id}")
                self.sessions[session_id] = [
                    Dequeue(),
                    threading.BoundedSemaphore(conf().get("concurrency_in_session", 4)),
                ]
            logger.debug(f"[chat_channel] Adding context to session queue: {session_id}")
            # 根据消息类型决定添加位置
            if context.type == ContextType.TEXT and context.content.startswith("#"):
                self.sessions[session_id][0].putleft(context)  # 优先处理管理命令
                logger.debug(f"[chat_channel] Added admin command to front of queue: {session_id}")
            else:
                self.sessions[session_id][0].put(context)  # 普通消息添加到队尾
                logger.debug(f"[chat_channel] Added normal message to queue: {session_id}")

    # 消费者函数，单独线程，用于从消息队列中取出消息并处理
    def consume(self):
        """消费者函数，处理会话队列中的消息"""
        logger.debug("[chat_channel] Starting consume loop")
        while True:
            with self.lock:
                session_ids = list(self.sessions.keys())
            if session_ids:
                logger.debug(f"[chat_channel] Found sessions: {session_ids}")
            for session_id in session_ids:
                with self.lock:
                    context_queue, semaphore = self.sessions[session_id]
                if semaphore.acquire(blocking=False):
                    try:
                        if not context_queue.empty():
                            context = context_queue.get()
                            logger.debug("[chat_channel] Processing context from queue: {}".format(context))
                            self._handle(context)
                        elif semaphore._initial_value == semaphore._value + 1:
                            with self.lock:
                                logger.debug(f"[chat_channel] Removing empty session: {session_id}")
                                del self.sessions[session_id]
                    finally:
                        semaphore.release()  # 确保信号量被释放
            time.sleep(0.2)

    # 取消session_id对应的所有任务，只能取消排队的消息和已提交线程池但未执行的任务
    def cancel_session(self, session_id):
        with self.lock:
            if session_id in self.sessions:
                for future in self.futures[session_id]:
                    future.cancel()
                cnt = self.sessions[session_id][0].qsize()
                if cnt > 0:
                    logger.info("Cancel {} messages in session {}".format(cnt, session_id))
                self.sessions[session_id][0] = Dequeue()

    def cancel_all_session(self):
        with self.lock:
            for session_id in self.sessions:
                for future in self.futures[session_id]:
                    future.cancel()
                cnt = self.sessions[session_id][0].qsize()
                if cnt > 0:
                    logger.info("Cancel {} messages in session {}".format(cnt, session_id))
                self.sessions[session_id][0] = Dequeue()

    def reset_session(self, session_id):
        """重置会话状态"""
        with self.lock:
            try:
                # 清理会话队列
                if session_id in self.sessions:
                    del self.sessions[session_id]
                
                # 取消未完成的Future
                if session_id in self.futures:
                    for future in self.futures[session_id]:
                        future.cancel()
                    del self.futures[session_id]
                    
                # 清理context缓存
                if session_id in self.session_id_to_context:
                    del self.session_id_to_context[session_id]
                    
                logger.info(f"[Channel] Successfully reset session: {session_id}")
            except Exception as e:
                logger.error(f"[Channel] Failed to reset session {session_id}: {e}")

    def check_connection(self):
        """检查连接状态"""
        try:
            # 检查消息队列连接
            if hasattr(self, 'mq_client'):
                mq_status = self.mq_client.check_connection()
                if not mq_status:
                    logger.error("[Channel] Message queue connection lost")
                    return False
                    
            # 检查会话状态
            if len(self.sessions) > 0:
                # 清理超时的会话
                current_time = time.time()
                timeout_sessions = []
                with self.lock:
                    for session_id, (_, semaphore) in self.sessions.items():
                        if current_time - self.session_timestamps.get(session_id, 0) > 3600:  # 1小时超时
                            timeout_sessions.append(session_id)
                            
                for session_id in timeout_sessions:
                    self.reset_session(session_id)
                    
            return True
        except Exception as e:
            logger.error(f"[Channel] Connection check failed: {e}")
            return False

    def auto_reconnect(self, max_retries=3, retry_interval=10):
        """自动重连机制"""
        retry_count = 0
        while retry_count < max_retries:
            try:
                logger.info(f"[Channel] Attempting to reconnect... (attempt {retry_count + 1}/{max_retries})")
                
                # 重置连接状态
                if hasattr(self, 'mq_client'):
                    self.mq_client.reconnect()
                
                # 检查连接
                if self.check_connection():
                    logger.info("[Channel] Reconnection successful")
                    return True
                    
                retry_count += 1
                if retry_count < max_retries:
                    time.sleep(retry_interval)
            except Exception as e:
                logger.error(f"[Channel] Reconnection attempt failed: {e}")
                retry_count += 1
                if retry_count < max_retries:
                    time.sleep(retry_interval)
                    
            logger.error("[Channel] Failed to reconnect after maximum retries")
            return False

    def _init_session_timestamps(self):
        """初始化会话时间戳记录"""
        self.session_timestamps = {}
        self.lock = threading.Lock()

def check_prefix(content, prefix_list):
    if not prefix_list:
        return None
    for prefix in prefix_list:
        if content.startswith(prefix):
            return prefix
    return None


def check_contain(content, keyword_list):
    if not keyword_list:
        return None
    for ky in keyword_list:
        if content.find(ky) != -1:
            return True
    return None
