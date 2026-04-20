# coding:utf-8
import time
import logging
import redis
import psutil
import json
import requests
from datetime import datetime
from threading import Thread
import os
from config import *  # 读取统一配置

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('monitor_service')

# 全局监控状态
monitor_status = {
    'timestamp': None,
    'heartbeat_timestamp': None,
    'redis_local': {
        'connected': False,
        'message': ''
    },
    'redis_cloud': {
        'connected': False,
        'message': ''
    },
    'signal_receiver': {
        'running': False,
        'pid': None,
        'message': ''
    },
    'miniqmt': {
        'running': False,
        'pid': None,
        'message': ''
    },
    'system': {
        'cpu_percent': 0,
        'memory_percent': 0,
        'disk_percent': 0
    }
}

last_report_time = 0

def check_redis_connection(host, port, password=None):
    try:
        r = redis.StrictRedis(host=host, port=port, password=password, decode_responses=True, socket_timeout=3)
        r.ping()
        return True, '连接正常'
    except Exception as e:
        return False, f'连接失败: {e}'

def find_process_by_name(process_name):
    for proc in psutil.process_iter(['pid', 'name']):
        try:
            if process_name.lower() in proc.info['name'].lower():
                return proc
        except:
            pass
    return None

def find_process_by_cmdline(keyword):
    for proc in psutil.process_iter(['pid', 'cmdline']):
        try:
            cmdline = ' '.join(proc.info['cmdline'])
            if keyword in cmdline:
                return proc
        except:
            pass
    return None

def check_signal_receiver():
    try:
        proc = find_process_by_cmdline('miniqmt_redis.py')
        if proc:
            return True, proc.pid, '运行中'
        proc = find_process_by_cmdline('joinquant_to_qmt')
        if proc:
            return True, proc.pid, '运行中'
        return False, None, '未运行'
    except:
        return False, None, '检查失败'

def check_miniqmt():
    try:
        names = ['xtdata.exe', 'xttrader.exe', 'QMT.exe', 'miniQMT.exe']
        for name in names:
            proc = find_process_by_name(name)
            if proc:
                return True, proc.pid, f'{name} 运行中'
        return False, None, '未运行'
    except:
        return False, None, '检查失败'

def get_system_info():
    try:
        return {
            'cpu_percent': psutil.cpu_percent(interval=1),
            'memory_percent': psutil.virtual_memory().percent,
            'disk_percent': psutil.disk_usage('/').percent
        }
    except:
        return {'cpu_percent':0,'memory_percent':0,'disk_percent':0}

def report_to_cloud_redis(status_data):
    try:
        r = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, decode_responses=True)
        key = 'monitor:status'
        r.set(key, json.dumps(status_data, ensure_ascii=False))
        r.expire(key, 60)
        stream_key = 'monitor:history'
        r.xadd(stream_key, {'timestamp':status_data['timestamp'],'data':json.dumps(status_data, ensure_ascii=False)})
        return True
    except:
        return False

def monitor_loop():
    global last_report_time
    logger.info(f'监控服务启动 → 心跳{HEARTBEAT_INTERVAL}秒 | 上报{REPORT_INTERVAL//60}分钟')
    
    while True:
        now = datetime.now()
        current_time = now.strftime('%Y-%m-%d %H:%M:%S')
        current_ts = int(now.timestamp())
        
        monitor_status['timestamp'] = current_time
        monitor_status['heartbeat_timestamp'] = current_ts
        
        # 检查本地Redis
        local_ok, local_msg = check_redis_connection(LOCAL_REDIS_HOST, LOCAL_REDIS_PORT, LOCAL_REDIS_PASSWORD)
        monitor_status['redis_local']['connected'] = local_ok
        monitor_status['redis_local']['message'] = local_msg
        
        # 检查云端Redis
        cloud_ok, cloud_msg = check_redis_connection(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)
        monitor_status['redis_cloud']['connected'] = cloud_ok
        monitor_status['redis_cloud']['message'] = cloud_msg
        
        # 检查进程
        recv_running, recv_pid, recv_msg = check_signal_receiver()
        monitor_status['signal_receiver']['running'] = recv_running
        monitor_status['signal_receiver']['pid'] = recv_pid
        monitor_status['signal_receiver']['message'] = recv_msg
        
        miniqmt_running, miniqmt_pid, miniqmt_msg = check_miniqmt()
        monitor_status['miniqmt']['running'] = miniqmt_running
        monitor_status['miniqmt']['pid'] = miniqmt_pid
        monitor_status['miniqmt']['message'] = miniqmt_msg
        
        # 系统信息
        monitor_status['system'] = get_system_info()
        
        # 上报控制
        if current_ts - last_report_time >= REPORT_INTERVAL or last_report_time == 0:
            report_to_cloud_redis(monitor_status)
            last_report_time = current_ts
            logger.info('✅ 已上报云端')
        
        time.sleep(HEARTBEAT_INTERVAL)

if __name__ == '__main__':
    t = Thread(target=monitor_loop, daemon=True)
    t.start()
    while True:
        time.sleep(1)