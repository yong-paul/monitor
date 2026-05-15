# coding:utf-8
"""
Cloud Service - Flask Web应用主程序（增强版）

增强功能：
1. 交易详情API - 获取单笔交易的完整信息
2. 分类统计API - 按策略、交易类型、状态、日期统计交易
3. 性能统计API - 获取策略收益统计
4. 消息追踪API - 获取消息处理状态和历史
5. WebSocket支持 - 实时推送消息

API接口：
- GET / - 主页（监控面板）
- GET /api/status - 获取云端Redis和策略流状态
- GET /api/monitor - 获取本地监控服务上报的状态
- GET /api/trades - 获取最近交易记录
- GET /api/trades/detail/<order_id> - 获取单笔交易详情
- GET /api/trades/categories - 获取交易分类统计
- GET /api/performance/<strategy_name> - 获取策略收益统计
- GET /api/message/tracking - 获取消息跟踪统计
- GET /api/streams/<stream_name> - 获取指定Stream的数据
"""

from flask import Flask, render_template, jsonify, request
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
    """获取Redis客户端连接"""
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
        print("Redis连接失败: {}".format(e))
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
    """获取系统状态API"""
    r = get_redis_client()
    
    if r:
        status_data['redis_connected'] = True
        try:
            stream_keys = r.keys('*')
            status_data['streams'] = {}
            for key in stream_keys:
                try:
                    key_type = r.type(key)
                    # 排除内部流：monitor:history, monitor:trades, monitor:status, message:tracking
                    if key_type == 'stream' and not key.startswith('monitor:') and not key.startswith('message:'):
                        stream_length = r.xlen(key)
                        last_msg = r.xrevrange(key, count=1)
                        last_id = last_msg[0][0] if last_msg else '0-0'
                        
                        # 获取全量消息
                        recent_messages = r.xrevrange(key, count=None)
                        messages = []
                        for msg_id, msg_data in recent_messages:
                            messages.append({
                                'id': msg_id,
                                'data': msg_data
                            })
                        
                        status_data['streams'][key] = {
                            'length': stream_length,
                            'last_generated_id': last_id,
                            'messages': messages
                        }
                except Exception as e:
                    continue
        except Exception as e:
            print("获取Stream信息失败: {}".format(e))
            status_data['streams'] = {}
    else:
        status_data['redis_connected'] = False
    
    status_data['last_update'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return jsonify(status_data)


@app.route('/api/strategy-flows')
def api_strategy_flows():
    """
    获取策略流数据（分页支持）
    
    返回所有策略流消息，支持分页、筛选和搜索。
    
    参数：
    - page: 页码（从0开始）
    - limit: 每页数量（默认20）
    - strategy: 策略名称过滤
    - action: 操作类型过滤（BUY/SELL）
    - search: 搜索关键词（匹配合约号或策略名称）
    
    返回：
    - data: 当前页数据
    - total: 总记录数
    - page: 当前页码
    - limit: 每页数量
    - has_more: 是否有更多数据
    """
    r = get_redis_client()
    if not r:
        return jsonify({'error': 'Redis连接失败'}), 500
    
    try:
        # 获取分页参数
        page = int(request.args.get('page', 0))
        limit = int(request.args.get('limit', 20))
        strategy_filter = request.args.get('strategy', '').strip()
        action_filter = request.args.get('action', '').strip().upper()
        search_query = request.args.get('search', '').strip().lower()
        
        # 获取所有策略流消息
        all_messages = []
        keys = r.keys('*')
        
        for key in keys:
            # 排除内部流：monitor:history, monitor:trades, monitor:status, message:tracking
            if key.startswith('monitor:') or key.startswith('message:'):
                continue
            
            try:
                if r.type(key) == 'stream':
                    # 策略名称过滤
                    if strategy_filter and key != strategy_filter:
                        continue
                    
                    # 获取所有消息
                    messages = r.xrevrange(key, count=None)
                    for msg_id, data in messages:
                        # 操作类型过滤
                        action = (data.get('action', '') or '').upper()
                        if action_filter and action != action_filter:
                            continue
                        
                        # 搜索过滤
                        code = (data.get('code', '') or '').lower()
                        strategy_name = key.lower()
                        if search_query and search_query not in code and search_query not in strategy_name:
                            continue
                        
                        all_messages.append({
                            'id': msg_id,
                            'strategy': key,
                            'code': data.get('code', ''),
                            'stock_code': data.get('stock_code', ''),
                            'action': action,
                            'order_volume': data.get('order_volume', ''),
                            'position_volume': data.get('position_volume', ''),
                            'status': data.get('status', ''),
                            'create_time': data.get('time', '')
                        })
            except Exception as e:
                continue
        
        # 按时间排序（最新的在前）
        all_messages.sort(key=lambda x: x['create_time'] or '', reverse=True)
        
        # 分页计算
        total = len(all_messages)
        start = page * limit
        end = start + limit
        paginated_data = all_messages[start:end]
        
        return jsonify({
            'data': paginated_data,
            'total': total,
            'page': page,
            'limit': limit,
            'has_more': end < total
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/strategy-flows/list')
def api_strategy_flows_list():
    """
    获取策略流名称列表
    
    返回所有策略流的名称，用于筛选下拉框。
    """
    r = get_redis_client()
    if not r:
        return jsonify({'error': 'Redis连接失败'}), 500
    
    try:
        streams = []
        keys = r.keys('*')
        
        for key in keys:
            if key.startswith('monitor:') or key.startswith('message:'):
                continue
            
            try:
                if r.type(key) == 'stream':
                    streams.append(key)
            except Exception as e:
                continue
        
        streams.sort()
        return jsonify(streams)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/monitor')
def api_monitor():
    """获取本地监控服务上报的状态API"""
    r = get_redis_client()
    if not r:
        return jsonify({'error': 'Redis连接失败'}), 500
    
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
    """获取最近的交易记录API"""
    r = get_redis_client()
    trades = []
    
    if r:
        try:
            messages = r.xrevrange('monitor:trades', count=50)
            for msg_id, data in messages:
                if 'data' in data:
                    try:
                        trade_data = json.loads(data['data'])
                        trades.append(trade_data)
                    except:
                        trades.append(data)
        except Exception as e:
            print("从Redis获取交易记录失败: {}".format(e))
    
    if not trades:
        trades = []
    return jsonify(trades)


@app.route('/api/trades/detail/<order_id>')
def api_trade_detail(order_id):
    """
    获取单笔交易详情API
    
    :param order_id: 订单ID
    :return: 交易详情JSON
    """
    r = get_redis_client()
    if not r:
        return jsonify({'error': 'Redis连接失败'}), 500
    
    try:
        # 从monitor:trades获取完整交易记录
        messages = r.xrevrange('monitor:trades', count=100)
        for msg_id, data in messages:
            if data.get('order_id') == order_id:
                # 解析嵌套的data字段
                if 'data' in data:
                    try:
                        parsed_data = json.loads(data['data'])
                        return jsonify({
                            'order_id': order_id,
                            'timestamp': data.get('timestamp', parsed_data.get('timestamp')),
                            'strategy': parsed_data.get('strategy'),
                            'stock_code': parsed_data.get('stock_code'),
                            'action': parsed_data.get('action'),
                            'price': parsed_data.get('price'),
                            'volume': parsed_data.get('volume'),
                            'amount': parsed_data.get('amount'),
                            'commission': parsed_data.get('commission', 0),
                            'status': parsed_data.get('status'),
                            'qmt_order_id': parsed_data.get('qmt_order_id'),
                            'filled_price': parsed_data.get('filled_price'),
                            'filled_volume': parsed_data.get('filled_volume'),
                            'slippage': parsed_data.get('slippage'),
                            'position_change': parsed_data.get('position_change'),
                            'funds_change': parsed_data.get('funds_change'),
                            'message_id': msg_id
                        })
                    except:
                        return jsonify({
                            'order_id': order_id,
                            'timestamp': data.get('timestamp'),
                            'data': data
                        })
        
        return jsonify({'error': '交易记录不存在'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/trades/categories')
def api_trade_categories():
    """
    获取交易分类统计API
    
    返回按策略、交易类型、状态、日期分组的统计数据。
    """
    r = get_redis_client()
    if not r:
        return jsonify({'error': 'Redis连接失败'}), 500
    
    try:
        messages = r.xrevrange('monitor:trades', count=1000)
        categories = {
            'by_strategy': {},
            'by_action': {'BUY': 0, 'SELL': 0},
            'by_status': {'filled': 0, 'pending': 0, 'partial': 0, 'failed': 0},
            'by_date': {},
            'total_count': 0,
            'total_amount': 0
        }
        
        for msg_id, data in messages:
            if 'data' in data:
                try:
                    parsed_data = json.loads(data['data'])
                    strategy = parsed_data.get('strategy', 'unknown')
                    action = parsed_data.get('action', '')
                    status = parsed_data.get('status', 'unknown')
                    timestamp = parsed_data.get('timestamp', '')
                    amount = float(parsed_data.get('traded_amount', 0))
                    
                    # 按策略分类
                    categories['by_strategy'][strategy] = categories['by_strategy'].get(strategy, 0) + 1
                    
                    # 按操作分类
                    if action in categories['by_action']:
                        categories['by_action'][action] += 1
                    
                    # 按状态分类
                    if status in categories['by_status']:
                        categories['by_status'][status] += 1
                    
                    # 按日期分类
                    if timestamp:
                        date = timestamp.split(' ')[0]
                        categories['by_date'][date] = categories['by_date'].get(date, {'count': 0, 'amount': 0})
                        categories['by_date'][date]['count'] += 1
                        categories['by_date'][date]['amount'] += amount
                    
                    # 总计
                    categories['total_count'] += 1
                    categories['total_amount'] += amount
                    
                except Exception as e:
                    continue
        
        # 转换日期统计格式
        categories['by_date'] = [
            {'date': date, 'count': stats['count'], 'amount': stats['amount']}
            for date, stats in sorted(categories['by_date'].items(), reverse=True)
        ]
        
        return jsonify(categories)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/performance/<strategy_name>')
def api_strategy_performance(strategy_name):
    """
    获取策略收益统计API
    
    :param strategy_name: 策略名称
    :return: 策略性能统计JSON
    """
    r = get_redis_client()
    if not r:
        return jsonify({'error': 'Redis连接失败'}), 500
    
    try:
        messages = r.xrevrange('monitor:trades', count=500)
        performance = {
            'strategy': strategy_name,
            'total_trades': 0,
            'total_buy': 0,
            'total_sell': 0,
            'total_amount': 0,
            'profit': 0,
            'profit_rate': 0,
            'daily_data': [],
            'positions': {}
        }
        
        # 按日期分组统计
        daily_stats = {}
        
        for msg_id, data in messages:
            if 'data' in data:
                try:
                    parsed_data = json.loads(data['data'])
                    if parsed_data.get('strategy') == strategy_name:
                        performance['total_trades'] += 1
                        action = parsed_data.get('action')
                        amount = float(parsed_data.get('traded_amount', 0))
                        price = float(parsed_data.get('traded_price', 0))
                        volume = int(parsed_data.get('traded_volume', 0))
                        timestamp = parsed_data.get('timestamp', '')
                        
                        if action == 'BUY':
                            performance['total_buy'] += 1
                            performance['total_amount'] += amount
                            # 更新持仓
                            stock_code = parsed_data.get('stock_code', '')
                            if stock_code:
                                if stock_code not in performance['positions']:
                                    performance['positions'][stock_code] = {
                                        'volume': 0,
                                        'avg_cost': 0,
                                        'current_price': price
                                    }
                                pos = performance['positions'][stock_code]
                                total_volume = pos['volume'] + volume
                                pos['avg_cost'] = (pos['avg_cost'] * pos['volume'] + price * volume) / total_volume
                                pos['volume'] = total_volume
                                pos['current_price'] = price
                            
                        elif action == 'SELL':
                            performance['total_sell'] += 1
                            performance['total_amount'] += amount
                            # 计算收益（简化计算）
                            stock_code = parsed_data.get('stock_code', '')
                            if stock_code in performance['positions']:
                                pos = performance['positions'][stock_code]
                                profit = (price - pos['avg_cost']) * volume
                                performance['profit'] += profit
                                pos['volume'] -= volume
                                if pos['volume'] <= 0:
                                    del performance['positions'][stock_code]
                        
                        # 按日期统计
                        if timestamp:
                            date = timestamp.split(' ')[0]
                            if date not in daily_stats:
                                daily_stats[date] = {'trades': 0, 'amount': 0, 'profit': 0}
                            daily_stats[date]['trades'] += 1
                            daily_stats[date]['amount'] += amount
                            
                except Exception as e:
                    continue
        
        # 计算收益率
        if performance['total_amount'] > 0:
            performance['profit_rate'] = (performance['profit'] / performance['total_amount']) * 100
        
        # 转换每日统计
        performance['daily_data'] = [
            {'date': date, 'trades': stats['trades'], 'amount': stats['amount']}
            for date, stats in sorted(daily_stats.items(), reverse=True)
        ]
        
        return jsonify(performance)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/message/tracking')
def api_message_tracking():
    """
    获取消息跟踪统计API
    
    返回消息处理状态的汇总统计和最近的消息跟踪记录。
    """
    r = get_redis_client()
    if not r:
        return jsonify({'error': 'Redis连接失败'}), 500
    
    try:
        messages = r.xrevrange('message:tracking', count=100)
        summary = {
            'total': 0,
            'by_status': {'pending': 0, 'processing': 0, 'success': 0, 'failed': 0, 'retried': 0},
            'recent_messages': []
        }
        
        for msg_id, data in messages:
            status = data.get('status', 'unknown')
            summary['total'] += 1
            
            if status in summary['by_status']:
                summary['by_status'][status] += 1
            
            # 最近的消息记录
            if len(summary['recent_messages']) < 20:
                details = {}
                if 'details' in data:
                    try:
                        details = json.loads(data['details'])
                    except:
                        pass
                
                summary['recent_messages'].append({
                    'msg_id': data.get('msg_id', msg_id),
                    'status': status,
                    'timestamp': data.get('timestamp'),
                    'details': details
                })
        
        return jsonify(summary)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/streams/<stream_name>')
def api_stream_data(stream_name):
    """获取指定Stream的数据API"""
    r = get_redis_client()
    if not r:
        return jsonify({'error': 'Redis连接失败'}), 500
    
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


@app.route('/api/monitor-history')
def api_monitor_history():
    """
    获取监控消息历史API
    
    返回最近的监控消息记录，包括系统状态变化、连接状态等。
    
    支持两种数据格式：
    1. monitor_service上报格式: {'timestamp': '...', 'data': 'JSON字符串'}
    2. 通用消息格式: {'timestamp': '...', 'type': '...', 'title': '...', 'message': '...', 'details': {...}}
    """
    r = get_redis_client()
    if not r:
        return jsonify({'error': 'Redis连接失败'}), 500
    
    try:
        messages = r.xrevrange('monitor:history', count=20)
        history = []
        
        for msg_id, data in messages:
            # 确保data是字典类型
            if not isinstance(data, dict):
                data = {}
            
            # 解析monitor_service上报的数据格式
            # monitor_service的格式: {'timestamp': '...', 'data': 'JSON字符串'}
            if 'data' in data and data.get('data'):
                try:
                    parsed_data = json.loads(data.get('data'))
                    history.append(parse_monitor_status(msg_id, data.get('timestamp'), parsed_data))
                except (json.JSONDecodeError, TypeError) as e:
                    # 解析失败，尝试其他格式
                    history.append({
                        'id': msg_id,
                        'timestamp': data.get('timestamp', ''),
                        'type': 'error',
                        'title': '数据解析失败',
                        'message': f'无法解析消息内容: {str(e)}',
                        'details': {'raw_data': str(data.get('data', '')[:500])}
                    })
            else:
                # 通用消息格式
                details = {}
                if data.get('details'):
                    try:
                        details = json.loads(data.get('details'))
                    except (json.JSONDecodeError, TypeError):
                        details = {'raw': str(data.get('details'))}
                
                history.append({
                    'id': msg_id,
                    'timestamp': data.get('timestamp', ''),
                    'type': data.get('type', 'info'),
                    'title': data.get('title', ''),
                    'message': data.get('message', ''),
                    'details': details
                })
        
        # 如果没有监控历史记录，返回模拟数据以便测试
        if not history:
            history = get_mock_monitor_history()
        
        return jsonify(history)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


def parse_monitor_status(msg_id, timestamp, status_data):
    """
    解析monitor_service上报的监控状态数据，转换为前端展示格式
    
    :param msg_id: 消息ID
    :param timestamp: 时间戳
    :param status_data: 监控状态数据
    :return: 格式化后的消息对象
    """
    # 分析系统状态，生成相应的消息
    issues = []
    status_details = {}
    
    # 检查Redis状态
    redis_local = status_data.get('redis_local', {})
    redis_cloud = status_data.get('redis_cloud', {})
    
    if not redis_local.get('connected'):
        issues.append(f"本地Redis: {redis_local.get('message', '连接失败')}")
    if not redis_cloud.get('connected'):
        issues.append(f"云端Redis: {redis_cloud.get('message', '连接失败')}")
    
    # 检查进程状态
    signal_receiver = status_data.get('signal_receiver', {})
    miniqmt = status_data.get('miniqmt', {})
    
    if not signal_receiver.get('running'):
        issues.append(f"信号接收进程: {signal_receiver.get('message', '未运行')}")
    if not miniqmt.get('running'):
        issues.append(f"MiniQMT: {miniqmt.get('message', '未运行')}")
    
    # 获取系统资源信息
    system = status_data.get('system', {})
    
    # 构建详情信息
    status_details = {
        'redis_local': {
            'connected': redis_local.get('connected'),
            'message': redis_local.get('message')
        },
        'redis_cloud': {
            'connected': redis_cloud.get('connected'),
            'message': redis_cloud.get('message')
        },
        'signal_receiver': {
            'running': signal_receiver.get('running'),
            'pid': signal_receiver.get('pid'),
            'message': signal_receiver.get('message')
        },
        'miniqmt': {
            'running': miniqmt.get('running'),
            'pid': miniqmt.get('pid'),
            'message': miniqmt.get('message')
        },
        'system': {
            'cpu_percent': system.get('cpu_percent'),
            'memory_percent': system.get('memory_percent'),
            'disk_percent': system.get('disk_percent')
        }
    }
    
    # 根据问题严重程度确定消息类型
    if issues:
        if any('连接失败' in issue or '未运行' in issue for issue in issues):
            msg_type = 'error'
            title = '系统异常'
            message = ' | '.join(issues)
        else:
            msg_type = 'warning'
            title = '系统警告'
            message = ' | '.join(issues)
    else:
        msg_type = 'success'
        title = '系统正常'
        message = '所有服务运行正常'
    
    return {
        'id': msg_id,
        'timestamp': timestamp,
        'type': msg_type,
        'title': title,
        'message': message,
        'details': status_details
    }


def get_mock_monitor_history():
    """
    获取模拟的监控消息历史数据（用于测试）
    """
    import time
    now = int(time.time())
    return [
        {
            'id': f'{now}-0',
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(now)),
            'type': 'success',
            'title': '系统启动',
            'message': '量化交易系统监控服务已成功启动',
            'details': {
                'service': 'monitor',
                'status': 'running',
                'pid': 1234,
                'uptime': 0
            }
        },
        {
            'id': f'{now-60}-1',
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(now-60)),
            'type': 'info',
            'title': 'Redis连接',
            'message': '成功连接到Redis服务器',
            'details': {
                'host': 'redis.example.com',
                'port': 6379,
                'connected': True,
                'latency_ms': 5
            }
        },
        {
            'id': f'{now-120}-2',
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(now-120)),
            'type': 'warning',
            'title': '策略状态',
            'message': '部分策略正在初始化',
            'details': {
                'total_strategies': 3,
                'running': 2,
                'initializing': 1,
                'stopped': 0
            }
        },
        {
            'id': f'{now-180}-3',
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(now-180)),
            'type': 'error',
            'title': '连接异常',
            'message': 'MiniQMT连接暂时断开，正在自动重连',
            'details': {
                'service': 'miniqmt',
                'status': 'reconnecting',
                'attempt': 1,
                'max_retries': 3
            }
        },
        {
            'id': f'{now-300}-4',
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(now-300)),
            'type': 'success',
            'title': '消息处理',
            'message': '成功处理100条交易消息',
            'details': {
                'processed': 100,
                'success': 98,
                'failed': 2,
                'rate': '98%'
            }
        }
    ]


@app.route('/api/system-stats')
def api_system_stats():
    """
    获取系统统计信息API
    
    返回系统资源使用情况、消息处理统计等。
    """
    r = get_redis_client()
    if not r:
        return jsonify({'error': 'Redis连接失败'}), 500
    
    try:
        stats = {
            'redis_info': {},
            'message_stats': {
                'total_processed': 0,
                'success_count': 0,
                'failed_count': 0
            },
            'stream_stats': {},
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # 获取Redis信息
        try:
            info = r.info()
            stats['redis_info'] = {
                'used_memory': info.get('used_memory_human', 'N/A'),
                'connected_clients': info.get('connected_clients', 0),
                'uptime': info.get('uptime_in_seconds', 0),
                'keyspace_hits': info.get('keyspace_hits', 0),
                'keyspace_misses': info.get('keyspace_misses', 0)
            }
        except:
            pass
        
        # 获取消息跟踪统计
        try:
            tracking_messages = r.xrevrange('message:tracking', count=1000)
            for _, data in tracking_messages:
                stats['message_stats']['total_processed'] += 1
                if data.get('status') == 'success':
                    stats['message_stats']['success_count'] += 1
                elif data.get('status') == 'failed':
                    stats['message_stats']['failed_count'] += 1
        except:
            pass
        
        # 获取Stream统计
        try:
            stream_keys = [k for k in r.keys('*') if r.type(k) == 'stream']
            for key in stream_keys:
                stats['stream_stats'][key] = {
                    'length': r.xlen(key)
                }
        except:
            pass
        
        return jsonify(stats)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/health')
def api_health():
    """健康检查API"""
    r = get_redis_client()
    return jsonify({
        'status': 'healthy' if r else 'unhealthy',
        'redis_connected': bool(r),
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    })


if __name__ == '__main__':
    app.run(host=FLASK_HOST, port=FLASK_PORT, debug=True)