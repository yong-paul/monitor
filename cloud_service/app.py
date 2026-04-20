# coding:utf-8
from flask import Flask, render_template, jsonify
import redis
import json
import time
from datetime import datetime
from config import *  # 读取统一配置

app = Flask(__name__)

# 全局变量用于存储状态和数据
status_data = {
    'redis_connected': False,
    'last_update': None,
    'streams': {},
    'trades': []
}

def get_redis_client():
    """获取 Redis 客户端连接"""
    try:
        r = redis.StrictRedis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            decode_responses=True
        )
        r.ping()
        return r
    except Exception as e:
        print("Redis 连接失败: {}".format(e))
        return None

@app.route('/')
def index():
    """主页 - 可视化展示，把配置传给前端"""
    return render_template('index.html', 
                           refresh_interval=WEB_REFRESH_INTERVAL,
                           heartbeat_interval=HEARTBEAT_INTERVAL,
                           report_interval=REPORT_INTERVAL)

@app.route('/simple')
def simple():
    """简单测试页面"""
    return render_template('simple.html')

@app.route('/api/status')
def api_status():
    """获取系统状态 API"""
    r = get_redis_client()
    
    if r:
        status_data['redis_connected'] = True
        try:
            stream_keys = r.keys('*')
            status_data['streams'] = {}
            for key in stream_keys:
                try:
                    key_type = r.type(key)
                    if key_type == 'stream':
                        stream_length = r.xlen(key)
                        last_msg = r.xrevrange(key, count=1)
                        last_id = last_msg[0][0] if last_msg else '0-0'
                        
                        status_data['streams'][key] = {
                            'length': stream_length,
                            'last_generated_id': last_id
                        }
                except Exception as e:
                    continue
        except Exception as e:
            print("获取 Stream 信息失败: {}".format(e))
            status_data['streams'] = {}
    else:
        status_data['redis_connected'] = False
    
    status_data['last_update'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return jsonify(status_data)

@app.route('/api/monitor')
def api_monitor():
    """获取本地监控服务上报的状态 API"""
    r = get_redis_client()
    if not r:
        return jsonify({'error': 'Redis 连接失败'}), 500
    
    try:
        monitor_data = r.get('monitor:status')
        if monitor_data:
            data = json.loads(monitor_data)
            current_timestamp = int(time.time())
            heartbeat_timestamp = data.get('heartbeat_timestamp', 0)
            time_since_heartbeat = current_timestamp - heartbeat_timestamp
            
            data['heartbeat_status'] = {
                'current_timestamp': current_timestamp,
                'heartbeat_timestamp': heartbeat_timestamp,
                'time_since_heartbeat': time_since_heartbeat,
                'timeout': time_since_heartbeat > HEARTBEAT_TIMEOUT,
                'timeout_seconds': HEARTBEAT_TIMEOUT
            }
            return jsonify(data)
        else:
            return jsonify({'message': '暂无监控数据'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/trades')
def api_trades():
    """获取最近的交易记录 API"""
    r = get_redis_client()
    trades = []
    
    if r:
        try:
            messages = r.xrevrange('monitor:trades', count=50)
            for msg_id, data in messages:
                if 'data' in data:
                    trade_data = json.parse(data['data'])
                    trades.append(trade_data)
        except Exception as e:
            print("从 Redis 获取交易记录失败: {}".format(e))
    
    if not trades:
        trades = [
            {
                'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'strategy': 'g9small',
                'code': '511880.SH',
                'action': 'BUY',
                'price': 100.00,
                'volume': 100
            }
        ]
    return jsonify(trades)

@app.route('/api/streams/<stream_name>')
def api_stream_data(stream_name):
    """获取指定 Stream 的数据 API"""
    r = get_redis_client()
    if not r:
        return jsonify({'error': 'Redis 连接失败'}), 500
    
    try:
        messages = r.xrevrange(stream_name, count=10)
        result = []
        for msg_id, data in messages:
            result.append({
                'id': msg_id,
                'data': data
            })
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host=FLASK_HOST, port=FLASK_PORT, debug=FLASK_DEBUG)