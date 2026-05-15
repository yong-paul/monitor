# coding:utf-8
"""
消息处理器模块 - 实现消息确认机制和幂等性处理

该模块提供以下核心功能：
1. 消息幂等性保障 - 确保每条消息只被处理一次
2. 分布式锁机制 - 防止并发处理同一消息
3. 消息重试机制 - 处理失败时自动重试
4. 消息确认机制 - 仅在处理成功后才从Stream删除

设计思路：
- 使用Redis的SET命令实现分布式锁（带超时自动释放）
- 使用Redis的字符串键存储已处理消息ID（带过期时间）
- 实现最大重试次数限制，避免无限重试
- 采用先处理后删除的策略，确保消息不丢失

核心数据结构：
- processed:{msg_id} - 标记消息已处理（24小时过期）
- processing:{msg_id} - 分布式锁（10秒过期）
- retry:{msg_id} - 重试计数器
"""

import json
import time
import logging
from typing import Optional, Dict, Any, Callable

logger = logging.getLogger(__name__)


class MessageProcessor:
    """
    消息处理器类
    
    负责从Redis Stream读取消息，确保消息被正确处理且仅处理一次。
    实现了完整的消息确认机制和幂等性保障。
    """
    
    def __init__(self, redis_client):
        """
        初始化消息处理器
        
        :param redis_client: Redis客户端实例
        """
        self.redis_client = redis_client
        
        # 配置参数
        self.MAX_RETRIES = 3              # 最大重试次数
        self.RETRY_DELAY_BASE = 60        # 基础重试间隔（秒）
        self.LOCK_TIMEOUT = 10            # 分布式锁超时时间（秒）
        self.PROCESSED_TTL = 86400        # 已处理消息记录过期时间（秒），24小时
        
        # Redis键前缀
        self.PREFIX_PROCESSED = 'processed:'
        self.PREFIX_LOCK = 'processing:'
        self.PREFIX_RETRY = 'retry:'
        
        # 本地缓存已处理消息ID（减少Redis查询）
        self._processed_cache = set()
        self._cache_max_size = 10000      # 本地缓存最大容量
    
    def _add_to_cache(self, msg_id: str) -> None:
        """
        将消息ID添加到本地缓存
        
        :param msg_id: 消息ID
        """
        if len(self._processed_cache) >= self._cache_max_size:
            # 缓存满时移除最早的一半
            self._processed_cache = set(list(self._processed_cache)[-self._cache_max_size//2:])
        self._processed_cache.add(msg_id)
    
    def _is_processed(self, msg_id: str) -> bool:
        """
        检查消息是否已处理
        
        优先检查本地缓存，再检查Redis，实现二级缓存策略。
        
        :param msg_id: 消息ID
        :return: 是否已处理
        """
        # 1. 先检查本地缓存（O(1)查询）
        if msg_id in self._processed_cache:
            logger.debug(f"消息 {msg_id} 在本地缓存中已标记为已处理")
            return True
        
        # 2. 再检查Redis（网络IO）
        key = f"{self.PREFIX_PROCESSED}{msg_id}"
        if self.redis_client.get(key):
            # 更新到本地缓存
            self._add_to_cache(msg_id)
            logger.debug(f"消息 {msg_id} 在Redis中已标记为已处理")
            return True
        
        return False
    
    def _acquire_lock(self, msg_id: str) -> bool:
        """
        获取分布式锁
        
        使用Redis SET命令的NX和EX参数实现：
        - NX: 只有键不存在时才设置
        - EX: 设置键的过期时间
        
        :param msg_id: 消息ID
        :return: 是否获取锁成功
        """
        lock_key = f"{self.PREFIX_LOCK}{msg_id}"
        # SET key value NX EX seconds
        result = self.redis_client.set(lock_key, "1", nx=True, ex=self.LOCK_TIMEOUT)
        success = result is not None
        if success:
            logger.debug(f"成功获取消息 {msg_id} 的处理锁")
        else:
            logger.debug(f"消息 {msg_id} 的处理锁已被其他进程持有")
        return success
    
    def _release_lock(self, msg_id: str) -> None:
        """
        释放分布式锁
        
        :param msg_id: 消息ID
        """
        lock_key = f"{self.PREFIX_LOCK}{msg_id}"
        self.redis_client.delete(lock_key)
        logger.debug(f"已释放消息 {msg_id} 的处理锁")
    
    def _mark_processed(self, msg_id: str) -> None:
        """
        标记消息已处理
        
        同时更新本地缓存和Redis，确保数据一致性。
        
        :param msg_id: 消息ID
        """
        # 1. 更新本地缓存
        self._add_to_cache(msg_id)
        
        # 2. 更新Redis（持久化存储）
        key = f"{self.PREFIX_PROCESSED}{msg_id}"
        self.redis_client.set(key, "1", ex=self.PROCESSED_TTL)
        
        # 3. 删除重试计数器（如果存在）
        retry_key = f"{self.PREFIX_RETRY}{msg_id}"
        self.redis_client.delete(retry_key)
        
        logger.debug(f"消息 {msg_id} 已标记为已处理")
    
    def _acknowledge(self, stream_name: str, msg_id: str) -> None:
        """
        确认消息（从Stream删除）
        
        只有在消息成功处理后才调用此方法。
        
        :param stream_name: Stream名称
        :param msg_id: 消息ID
        """
        try:
            self.redis_client.xdel(stream_name, msg_id)
            logger.debug(f"消息 {msg_id} 已从Stream {stream_name} 删除")
        except Exception as e:
            # 消息可能已经被删除或不存在于Stream中，忽略此错误
            logger.debug(f"尝试删除消息 {msg_id} 失败（可能已不存在）: {str(e)}")
    
    def _get_retry_count(self, msg_id: str) -> int:
        """
        获取消息的重试次数
        
        :param msg_id: 消息ID
        :return: 重试次数
        """
        retry_key = f"{self.PREFIX_RETRY}{msg_id}"
        count = self.redis_client.get(retry_key)
        return int(count) if count else 0
    
    def _increment_retry_count(self, msg_id: str) -> int:
        """
        增加重试次数
        
        :param msg_id: 消息ID
        :return: 更新后的重试次数
        """
        retry_key = f"{self.PREFIX_RETRY}{msg_id}"
        # 使用INCR命令原子性增加计数
        count = self.redis_client.incr(retry_key)
        
        # 设置过期时间，防止重试计数器永久存在
        # 过期时间随重试次数递增：第N次重试后等待 N * BASE_DELAY 秒
        ttl = self.RETRY_DELAY_BASE * count
        self.redis_client.expire(retry_key, ttl)
        
        return count
    
    def _schedule_retry(self, stream_name: str, msg_id: str, msg_data: Dict[str, Any]) -> None:
        """
        安排消息重试
        
        将消息重新放入Stream尾部，等待下次处理。
        
        :param stream_name: Stream名称
        :param msg_id: 消息ID
        :param msg_data: 消息数据
        """
        retry_count = self._increment_retry_count(msg_id)
        
        if retry_count < self.MAX_RETRIES:
            # 还有重试机会，将消息重新放入Stream尾部
            try:
                self.redis_client.xadd(stream_name, msg_data, maxlen=1000)
            except Exception as e:
                logger.debug(f"将消息 {msg_id} 重新放入Stream失败（可能Stream不存在）: {str(e)}")
            logger.warning(f"消息 {msg_id} 处理失败，已安排第 {retry_count}/{self.MAX_RETRIES} 次重试")
        else:
            # 达到最大重试次数，记录错误日志并标记为已处理
            logger.error(f"消息 {msg_id} 达到最大重试次数({self.MAX_RETRIES})，已放弃处理")
            # 标记为已处理，避免重复尝试
            self._mark_processed(msg_id)
    
    def process_message(
        self, 
        stream_name: str, 
        msg_id: str, 
        msg_data: Dict[str, Any], 
        handler: Callable[[Dict[str, Any]], bool]
    ) -> bool:
        """
        处理消息，保证幂等性
        
        完整的消息处理流程：
        1. 检查消息是否已处理（幂等性保障）
        2. 获取分布式锁（并发控制）
        3. 执行业务逻辑
        4. 标记消息已处理
        5. 确认消息（从Stream删除）
        
        :param stream_name: Stream名称
        :param msg_id: 消息ID
        :param msg_data: 消息数据
        :param handler: 业务处理函数，返回True表示成功，False表示失败
        :return: 是否处理成功
        """
        # 1. 检查是否已处理（幂等性保障）
        if self._is_processed(msg_id):
            logger.debug(f"消息 {msg_id} 已处理过，跳过")
            self._acknowledge(stream_name, msg_id)
            return True
        
        # 2. 获取分布式锁（并发控制）
        if not self._acquire_lock(msg_id):
            logger.debug(f"消息 {msg_id} 正在被其他进程处理，跳过")
            return False
        
        lock_acquired = True
        try:
            # 3. 再次检查是否已处理（防止在获取锁期间被其他进程处理）
            if self._is_processed(msg_id):
                logger.debug(f"消息 {msg_id} 在获取锁期间已被处理，跳过")
                self._acknowledge(stream_name, msg_id)
                return True
            
            # 4. 执行业务逻辑
            logger.debug(f"开始处理消息 {msg_id}")
            success = handler(msg_data)
            
            if success:
                # 5. 标记已处理
                self._mark_processed(msg_id)
                # 6. 确认消息（从Stream删除）
                self._acknowledge(stream_name, msg_id)
                logger.info(f"消息 {msg_id} 处理成功")
                return True
            else:
                # 7. 处理失败，安排重试
                self._schedule_retry(stream_name, msg_id, msg_data)
                return False
        
        except Exception as e:
            # 处理过程中发生异常
            logger.error(f"消息 {msg_id} 处理异常: {str(e)}")
            # 安排重试
            self._schedule_retry(stream_name, msg_id, msg_data)
            return False
        
        finally:
            # 确保锁被释放
            if lock_acquired:
                self._release_lock(msg_id)
    
    def batch_process(self, stream_name: str, messages: list, handler: Callable[[Dict[str, Any]], bool] = None) -> dict:
        """
        批量处理消息
        
        :param stream_name: Stream名称
        :param messages: 消息列表，格式为 [(msg_id, msg_data), ...]
        :param handler: 业务处理函数，返回True表示成功，False表示失败
        :return: 处理结果统计 {'success': 成功数, 'skipped': 跳过数, 'failed': 失败数}
        """
        result = {'success': 0, 'skipped': 0, 'failed': 0}
        # 使用传入的handler或默认handler
        actual_handler = handler if handler else self._default_handler
        
        for msg_id, msg_data in messages:
            try:
                # 将字节数据转换为字符串
                if isinstance(msg_data, dict):
                    processed_data = {}
                    for k, v in msg_data.items():
                        if isinstance(v, bytes):
                            processed_data[k] = v.decode('utf-8')
                        else:
                            processed_data[k] = v
                    msg_data = processed_data
                
                success = self.process_message(stream_name, msg_id, msg_data, actual_handler)
                
                if success:
                    result['success'] += 1
                else:
                    # 检查是被跳过还是处理失败
                    if self._is_processed(msg_id):
                        result['skipped'] += 1
                    else:
                        result['failed'] += 1
            except Exception as e:
                logger.error(f"批量处理消息 {msg_id} 时发生错误: {str(e)}")
                result['failed'] += 1
        
        return result
    
    def _default_handler(self, msg_data: Dict[str, Any]) -> bool:
        """
        默认消息处理函数（示例）
        
        在实际使用中，应替换为具体的业务处理逻辑。
        
        :param msg_data: 消息数据
        :return: 是否处理成功
        """
        logger.info(f"处理消息数据: {json.dumps(msg_data, ensure_ascii=False)}")
        return True


class MessageTracker:
    """
    消息追踪器
    
    用于跟踪消息的处理状态，提供消息处理的完整视图。
    """
    
    def __init__(self, redis_client):
        self.redis_client = redis_client
        self.STREAM_NAME = 'message:tracking'
    
    def track_message(self, msg_id: str, status: str, details: Dict[str, Any] = None) -> None:
        """
        记录消息处理状态
        
        :param msg_id: 消息ID
        :param status: 处理状态 (pending/processing/success/failed/retried)
        :param details: 详细信息
        """
        import datetime
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        data = {
            'msg_id': msg_id,
            'status': status,
            'timestamp': timestamp
        }
        
        if details:
            data['details'] = json.dumps(details, ensure_ascii=False)
        
        self.redis_client.xadd(self.STREAM_NAME, data, maxlen=10000)
        logger.debug(f"消息 {msg_id} 状态已更新为 {status}")
    
    def get_message_history(self, msg_id: str, limit: int = 20) -> list:
        """
        获取消息的处理历史
        
        :param msg_id: 消息ID
        :param limit: 返回记录数限制
        :return: 处理历史列表
        """
        messages = self.redis_client.xrevrange(self.STREAM_NAME, count=limit)
        history = []
        
        for msg_id_, data in messages:
            if data.get('msg_id') == msg_id:
                history.append({
                    'id': msg_id_,
                    'status': data.get('status'),
                    'timestamp': data.get('timestamp'),
                    'details': json.loads(data.get('details', '{}')) if data.get('details') else {}
                })
        
        return history
    
    def get_tracking_summary(self) -> dict:
        """
        获取消息跟踪汇总统计
        
        :return: 统计信息 {'total': 总数, 'by_status': 按状态统计}
        """
        messages = self.redis_client.xrevrange(self.STREAM_NAME, count=1000)
        summary = {
            'total': 0,
            'by_status': {}
        }
        
        for _, data in messages:
            status = data.get('status', 'unknown')
            summary['total'] += 1
            summary['by_status'][status] = summary['by_status'].get(status, 0) + 1
        
        return summary


# 示例用法
if __name__ == '__main__':
    import redis
    
    # 配置日志
    logging.basicConfig(level=logging.DEBUG)
    
    # 创建Redis客户端
    r = redis.StrictRedis(
        host='localhost',
        port=6379,
        decode_responses=True
    )
    
    # 创建消息处理器
    processor = MessageProcessor(r)
    
    # 创建消息追踪器
    tracker = MessageTracker(r)
    
    # 示例消息数据
    test_msg_data = {
        'time': '2024-01-02 09:38:00',
        'action': 'BUY',
        'code': '511010.XSHG',
        'pct': 0.0116,
        'strategy': 'test_strategy',
        'price': 116.409,
        'cancel_order': 0
    }
    
    # 示例处理函数
    def test_handler(msg_data):
        print(f"处理业务逻辑: {msg_data}")
        # 模拟90%成功率
        import random
        return random.random() < 0.9
    
    # 处理消息
    result = processor.process_message(
        stream_name='test_stream',
        msg_id='12345',
        msg_data=test_msg_data,
        handler=test_handler
    )
    print(f"处理结果: {result}")
    
    # 查看跟踪统计
    summary = tracker.get_tracking_summary()
    print(f"跟踪统计: {summary}")