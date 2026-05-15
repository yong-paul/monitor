# coding:utf-8
"""
消息处理器单元测试

测试覆盖：
1. 消息幂等性测试 - 确保同一条消息只被处理一次
2. 分布式锁测试 - 确保并发处理时消息只被处理一次
3. 消息重试测试 - 确保失败消息会被重试
4. 消息确认测试 - 确保成功处理后消息被确认
"""

import pytest
import redis
import time
import threading
from concurrent.futures import ThreadPoolExecutor

# 添加项目路径
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../local_service'))

from message_processor import MessageProcessor, MessageTracker


class TestMessageProcessor:
    """消息处理器单元测试类"""
    
    @pytest.fixture(autouse=True)
    def setup_teardown(self):
        """每个测试前后清理Redis数据"""
        # 创建测试用的Redis客户端
        self.redis_client = redis.StrictRedis(
            host='localhost',
            port=6379,
            decode_responses=True
        )
        
        # 清理测试数据
        self._clean_redis()
        
        yield
        
        # 测试后清理
        self._clean_redis()
    
    def _clean_redis(self):
        """清理测试相关的Redis数据"""
        keys = self.redis_client.keys('processed:*')
        if keys:
            self.redis_client.delete(*keys)
        
        keys = self.redis_client.keys('processing:*')
        if keys:
            self.redis_client.delete(*keys)
        
        keys = self.redis_client.keys('retry:*')
        if keys:
            self.redis_client.delete(*keys)
        
        # 删除测试Stream
        if self.redis_client.exists('test_stream'):
            self.redis_client.delete('test_stream')
        
        if self.redis_client.exists('message:tracking'):
            self.redis_client.delete('message:tracking')
    
    def test_idempotency(self):
        """测试消息幂等性 - 同一条消息处理多次只执行一次业务逻辑"""
        processor = MessageProcessor(self.redis_client)
        
        # 创建测试消息
        test_msg_id = 'test_idempotency_001'
        test_msg_data = {
            'action': 'BUY',
            'code': '600000.SH',
            'price': 10.0,
            'volume': 100
        }
        
        # 模拟业务处理函数，记录调用次数
        call_count = {'count': 0}
        
        def handler(msg_data):
            call_count['count'] += 1
            return True
        
        # 第一次处理
        result1 = processor.process_message('test_stream', test_msg_id, test_msg_data, handler)
        assert result1 == True
        assert call_count['count'] == 1
        
        # 第二次处理同一条消息（应该跳过）
        result2 = processor.process_message('test_stream', test_msg_id, test_msg_data, handler)
        assert result2 == True  # 返回True表示已处理（跳过）
        assert call_count['count'] == 1  # 业务逻辑应该只执行一次
        
        # 验证Redis中标记已处理
        assert self.redis_client.get(f'processed:{test_msg_id}') == '1'
    
    def test_distributed_lock(self):
        """测试分布式锁 - 并发处理时消息只被处理一次"""
        processor = MessageProcessor(self.redis_client)
        
        test_msg_id = 'test_lock_001'
        test_msg_data = {'action': 'BUY', 'code': '600000.SH'}
        
        call_count = {'count': 0}
        results = []
        
        def handler(msg_data):
            time.sleep(0.1)  # 模拟处理耗时
            call_count['count'] += 1
            return True
        
        # 创建多个线程并发处理同一条消息
        def process_in_thread():
            result = processor.process_message('test_stream', test_msg_id, test_msg_data, handler)
            results.append(result)
        
        threads = []
        for _ in range(5):
            t = threading.Thread(target=process_in_thread)
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        # 业务逻辑应该只执行一次（核心断言）
        assert call_count['count'] == 1
        # 至少有一个线程返回True（实际处理或发现已处理）
        assert sum(results) >= 1
        # 验证消息已被标记为已处理
        assert self.redis_client.get(f'processed:{test_msg_id}') == '1'
    
    def test_message_retry(self):
        """测试消息重试机制 - 失败消息会被重试"""
        processor = MessageProcessor(self.redis_client)
        processor.MAX_RETRIES = 3  # 设置3次重试，确保第二次失败后还有重试机会
        
        test_msg_id = 'test_retry_001'
        test_msg_data = {'action': 'BUY', 'code': '600000.SH'}
        
        call_count = {'count': 0}
        fail_twice = 2  # 前2次失败，之后成功
        
        def handler(msg_data):
            call_count['count'] += 1
            if call_count['count'] <= fail_twice:
                return False  # 失败
            return True  # 成功
        
        # 第一次处理（应该失败）
        result1 = processor.process_message('test_stream', test_msg_id, test_msg_data, handler)
        assert result1 == False
        assert call_count['count'] == 1
        
        # 检查重试计数器
        retry_count = int(self.redis_client.get(f'retry:{test_msg_id}') or '0')
        assert retry_count == 1
        
        # 第二次处理（仍然失败，但还有重试机会）
        result2 = processor.process_message('test_stream', test_msg_id, test_msg_data, handler)
        assert result2 == False
        assert call_count['count'] == 2
        
        # 检查重试计数器
        retry_count = int(self.redis_client.get(f'retry:{test_msg_id}') or '0')
        assert retry_count == 2
        
        # 第三次处理（handler返回True，应该成功）
        result3 = processor.process_message('test_stream', test_msg_id, test_msg_data, handler)
        assert result3 == True
        assert call_count['count'] == 3
        
        # 验证消息已被标记为已处理
        assert self.redis_client.get(f'processed:{test_msg_id}') == '1'
        # 重试计数器应该被删除
        assert self.redis_client.get(f'retry:{test_msg_id}') is None
    
    def test_max_retries_exceeded(self):
        """测试超过最大重试次数后消息被标记为已处理"""
        processor = MessageProcessor(self.redis_client)
        processor.MAX_RETRIES = 2
        
        test_msg_id = 'test_max_retry_001'
        test_msg_data = {'action': 'BUY', 'code': '600000.SH'}
        
        call_count = {'count': 0}
        
        def handler(msg_data):
            call_count['count'] += 1
            return False  # 总是失败
        
        # 第一次处理（失败，重试计数器=1，还有重试机会）
        result1 = processor.process_message('test_stream', test_msg_id, test_msg_data, handler)
        assert result1 == False
        assert call_count['count'] == 1
        assert int(self.redis_client.get(f'retry:{test_msg_id}') or '0') == 1
        
        # 第二次处理（失败，重试计数器=2，达到最大重试次数，消息被标记为已处理）
        result2 = processor.process_message('test_stream', test_msg_id, test_msg_data, handler)
        assert result2 == False  # 最后一次尝试仍然失败
        assert call_count['count'] == 2
        # 验证消息已被标记为已处理（在_schedule_retry中完成）
        assert self.redis_client.get(f'processed:{test_msg_id}') == '1'
        
        # 第三次处理（消息已被标记为已处理，跳过handler，返回True）
        result3 = processor.process_message('test_stream', test_msg_id, test_msg_data, handler)
        assert result3 == True  # 被标记为已处理（跳过）
        assert call_count['count'] == 2  # 不再调用业务逻辑
    
    def test_message_acknowledgment(self):
        """测试消息确认机制 - 成功处理后从Stream删除"""
        # 先创建测试Stream并添加消息
        self.redis_client.xadd('test_stream', {'data': 'test'})
        
        processor = MessageProcessor(self.redis_client)
        
        # 获取Stream中的消息
        messages = self.redis_client.xrevrange('test_stream', count=1)
        assert len(messages) == 1
        msg_id = messages[0][0]
        
        def handler(msg_data):
            return True
        
        # 处理消息
        result = processor.process_message('test_stream', msg_id, {}, handler)
        assert result == True
        
        # 验证消息已从Stream删除
        messages = self.redis_client.xrevrange('test_stream', count=1)
        assert len(messages) == 0
    
    def test_batch_process(self):
        """测试批量处理消息"""
        processor = MessageProcessor(self.redis_client)
        
        test_messages = [
            ('msg_001', {'action': 'BUY', 'code': '600000.SH'}),
            ('msg_002', {'action': 'SELL', 'code': '600001.SH'}),
            ('msg_003', {'action': 'BUY', 'code': '600002.SH'})
        ]
        
        call_count = {'count': 0}
        
        def handler(msg_data):
            call_count['count'] += 1
            return True
        
        # 批量处理（传入自定义handler）
        result = processor.batch_process('test_stream', test_messages, handler)
        
        assert result['success'] == 3
        assert result['skipped'] == 0
        assert result['failed'] == 0
        assert call_count['count'] == 3
        
        # 再次批量处理（应该全部跳过）
        result2 = processor.batch_process('test_stream', test_messages, handler)
        assert result2['success'] == 3  # 全部标记为已处理
        assert result2['skipped'] == 0
        assert result2['failed'] == 0
        assert call_count['count'] == 3  # 业务逻辑不再执行


class TestMessageTracker:
    """消息追踪器单元测试类"""
    
    @pytest.fixture(autouse=True)
    def setup_teardown(self):
        self.redis_client = redis.StrictRedis(
            host='localhost',
            port=6379,
            decode_responses=True
        )
        
        # 清理测试数据
        if self.redis_client.exists('message:tracking'):
            self.redis_client.delete('message:tracking')
        
        yield
        
        # 测试后清理
        if self.redis_client.exists('message:tracking'):
            self.redis_client.delete('message:tracking')
    
    def test_track_message(self):
        """测试记录消息处理状态"""
        tracker = MessageTracker(self.redis_client)
        
        tracker.track_message('test_msg_001', 'success', {'strategy': 'test_strategy'})
        
        # 验证消息已记录
        messages = self.redis_client.xrevrange('message:tracking', count=1)
        assert len(messages) == 1
        
        msg_data = messages[0][1]
        assert msg_data['msg_id'] == 'test_msg_001'
        assert msg_data['status'] == 'success'
    
    def test_get_message_history(self):
        """测试获取消息处理历史"""
        tracker = MessageTracker(self.redis_client)
        
        tracker.track_message('test_msg_002', 'processing')
        tracker.track_message('test_msg_002', 'success')
        tracker.track_message('other_msg', 'success')  # 不同的消息
        
        history = tracker.get_message_history('test_msg_002')
        
        assert len(history) == 2
        assert history[0]['status'] == 'success'
        assert history[1]['status'] == 'processing'
    
    def test_get_tracking_summary(self):
        """测试获取跟踪汇总统计"""
        tracker = MessageTracker(self.redis_client)
        
        tracker.track_message('msg1', 'success')
        tracker.track_message('msg2', 'success')
        tracker.track_message('msg3', 'failed')
        tracker.track_message('msg4', 'processing')
        
        summary = tracker.get_tracking_summary()
        
        assert summary['total'] == 4
        assert summary['by_status']['success'] == 2
        assert summary['by_status']['failed'] == 1
        assert summary['by_status']['processing'] == 1


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
