# coding:utf-8
"""
通过Redis的Stream模式获取交易信号，并驱动miniQMT下单

主要特性（优化后）：
1. 接收聚宽发送的百分比下单信号，并根据本地数据库中的资金量进行下单
2. 可同时监听多个策略的信号并下单，策略之间资金互相独立
3. Stream模式获取交易信号可避免网络等原因导致的信号丢失
4. 加入miniQMT的交易接口断开时自动重连逻辑
5. 下单时加入了时间差检查和滑点检查
6. **新增**：消息确认机制 - 确保每条消息只处理一次
7. **新增**：分布式锁机制 - 防止并发处理同一消息
8. **新增**：消息重试机制 - 处理失败时自动重试
9. **新增**：消息追踪系统 - 完整记录消息处理状态

注意：
    1) 使用前将redis.StrictRedis方法中的host,port,password以及miniQMT的账号信息改成自己的
    2) 下单依赖本地数据库，请使用db_manager.py文件中的create_strategy_table方法创建本地数据库和策略表
    3) 为保证成交，委托均为对手方最优价的市价委托（实际委托以涨停或跌停价格发布）
"""

import time, datetime, traceback, sys
import logging
import redis
from multiprocessing import Manager

from xtquant import xtdata, xttype
from xtquant.xttrader import XtQuantTrader, XtQuantTraderCallback
from xtquant.xttype import StockAccount
from xtquant import xtconstant

from db_manager import DatabaseManager
from message_processor import MessageProcessor, MessageTracker  # 导入消息处理器


# ==================== 配置参数 ====================
DB_NAME = 'strategy_data.db'
acc = StockAccount('your_account_id', 'STOCK')  # 填写你自己的账号id信息
path = r'D:\国金QMT交易端模拟\userdata_mini'  # 填写你自己的userdata_mini路径

time_check = True   # 是否检查下单时间差
SlippagePct = 0.01  # 买入时允许下单的滑点阈值

# 消息处理配置
MAX_RETRY_DELAY = 300  # 最大重试间隔（秒）
MESSAGE_PROCESSING_TIMEOUT = 30  # 消息处理超时时间（秒）


# ==================== 日志配置 ====================
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    fmt="%(asctime)s.%(msecs)03d - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
console_handler.setFormatter(formatter)

nowtime = time.strftime("%Y%m%d_%H%M%S")
logpath = fr".\log\qmt_{nowtime}.log"
file_handler = logging.FileHandler(logpath, mode="a", encoding="utf-8")
file_handler.setFormatter(formatter)

logger = logging.getLogger("joinquant_to_qmt")
logger.setLevel(logging.DEBUG)
logger.addHandler(console_handler)
logger.addHandler(file_handler)


# ==================== 全局状态容器 ====================
class _a():
    pass

A = _a()
A.bought_list = []
A.hsa = xtdata.get_stock_list_in_sector('沪深A股')


def interact():
    """执行后进入repl模式"""
    import code
    code.InteractiveConsole(locals=globals()).interact()


# ==================== 资金冻结处理器 ====================
class freeze_hanadler:
    """
    资金冻结处理器
    
    管理策略资金的冻结和解冻，确保下单过程中资金不会被重复使用。
    使用进程间共享字典存储冻结信息。
    """

    @staticmethod
    def pre_add_cash(freeze_dict, strategy_name, order_seq, vol, price):
        """
        预存入资金（卖出前准备）
        
        :param freeze_dict: 冻结字典（进程间共享）
        :param strategy_name: 策略名称
        :param order_seq: 下单序号
        :param vol: 数量
        :param price: 价格
        """
        ca = vol * price
        logger.debug(f"预存入策略{strategy_name}资金{vol}*{price}={ca}，order_seq：{order_seq}")
        strategy_dict = freeze_dict.setdefault(strategy_name, Manager().dict())
        strategy_dict[f"seq{order_seq}"] = [-vol, price]

    @staticmethod
    def freeze_cash(freeze_dict, strategy_name, order_seq, vol, price):
        """
        冻结资金（买入前）
        
        :param freeze_dict: 冻结字典（进程间共享）
        :param strategy_name: 策略名称
        :param order_seq: 下单序号
        :param vol: 数量
        :param price: 价格
        """
        ca = vol * price
        logger.debug(f"冻结策略{strategy_name}资金{vol}*{price}={ca}，order_seq：{order_seq}")
        strategy_dict = freeze_dict.setdefault(strategy_name, Manager().dict())
        strategy_dict[f"seq{order_seq}"] = [vol, price]

    @staticmethod
    def unfreeze_cash(freeze_dict, strategy_name, order_id, vol_change):
        """
        解冻资金（成交后）
        
        :param freeze_dict: 冻结字典（进程间共享）
        :param strategy_name: 策略名称
        :param order_id: 订单ID
        :param vol_change: 数量变化（买入为正，卖出为负）
        """
        strategy_dict = freeze_dict.get(strategy_name)
        logger.debug(f"当前冻结字典：{freeze_dict}，策略{strategy_name}冻结字典：{strategy_dict}，成交order_id：{order_id}")
        
        if strategy_dict:
            try:
                vol, price = strategy_dict.get(f"ord{order_id}")
                ca_change = vol_change * price
                
                if ca_change > 0:
                    logger.debug(f"解冻策略{strategy_name}资金{vol_change}*{price}={ca_change}，order_id：{order_id}")
                else:
                    logger.debug(f"删除策略{strategy_name}预存入资金{-vol_change}*{price}={-ca_change}，order_id：{order_id}")
                
                new_vol = vol - vol_change
                if new_vol == 0:
                    strategy_dict.pop(f"ord{order_id}")
                else:
                    strategy_dict[f"ord{order_id}"] = [new_vol, price]
            except Exception as e:
                logger.error(f"解冻资金失败: {e}")

    @staticmethod
    def change_seq_to_id(freeze_dict, strategy_name, order_seq, order_id):
        """
        将下单序号转换为订单ID
        
        :param freeze_dict: 冻结字典（进程间共享）
        :param strategy_name: 策略名称
        :param order_seq: 下单序号
        :param order_id: 订单ID
        """
        strategy_dict = freeze_dict.get(strategy_name)
        logger.debug(f"策略{strategy_name}冻结字典:{strategy_dict}，order_seq:{order_seq}, order_id:{order_id}")
        
        if strategy_dict and f"seq{order_seq}" in strategy_dict:
            strategy_dict[f"ord{order_id}"] = strategy_dict.pop(f"seq{order_seq}")

    @staticmethod
    def get_frozen_cash(freeze_dict, strategy_name):
        """
        获取策略的冻结资金总额
        
        :param freeze_dict: 冻结字典（进程间共享）
        :param strategy_name: 策略名称
        :return: 冻结资金总额
        """
        frozen_cash = 0
        strategy_dict = freeze_dict.get(strategy_name)
        
        if strategy_dict:
            for vol, price in strategy_dict.values():
                frozen_cash += abs(vol) * price
        
        logger.debug(f"策略{strategy_name}总计冻结资金{frozen_cash}")
        return frozen_cash


# ==================== 交易回调处理器 ====================
class MyXtQuantTraderCallback(XtQuantTraderCallback):
    """
    QMT交易回调处理器
    
    处理交易接口的各种回调事件，包括：
    - 连接断开
    - 委托回报
    - 成交回报
    - 委托失败
    - 撤单失败
    - 异步下单回报
    """

    def __init__(self, manager, db_name, xt_trader, freeze_dict):
        self.logger = logging.getLogger("MyXtQuantTraderCallback")
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)
        
        self.db_manager = manager
        self.db_name = db_name
        self.xt_trader = xt_trader
        self.freeze_dict = freeze_dict

    def on_disconnected(self):
        """连接断开回调"""
        self.logger.warning("连接断开回调: connection lost, 交易接口断开")
        self.xt_trader.connection_lost()

    def on_stock_order(self, order):
        """委托回报推送"""
        self.logger.info(f'委托回调:{order.order_remark}')

    def on_stock_trade(self, trade: xttype.XtTrade):
        """
        成交变动推送
        
        处理成交回报，更新持仓和资金，并记录成交记录到数据库。
        """
        self.logger.info(f'成交回调: {trade.order_remark}')
        
        with self.db_manager(self.db_name) as db_m:
            strategy = trade.strategy_name
            code = trade.stock_code
            filled_price = trade.traded_price
            filled_volume = trade.traded_volume
            traded_amount = trade.traded_amount
            action = trade.order_type
            order_id = trade.order_id
            commission = trade.commission
            
            self.logger.info(
                f"成交详情：strategy:{strategy},code:{code},filled_price:{filled_price},"
                f"filled_volume:{filled_volume},traded_amount:{traded_amount},action:{action},"
                f"order_id:{order_id},commission:{commission}"
            )
            
            # 更新持仓和资金
            if action == xtconstant.STOCK_BUY:
                db_m.update_position_and_funds(strategy, code, filled_volume, -traded_amount)
                freeze_hanadler.unfreeze_cash(self.freeze_dict, strategy, order_id, filled_volume)
                order_type_str = 'BUY'
            elif action == xtconstant.STOCK_SELL:
                db_m.update_position_and_funds(strategy, code, -filled_volume, traded_amount)
                freeze_hanadler.unfreeze_cash(self.freeze_dict, strategy, order_id, -filled_volume)
                order_type_str = 'SELL'
            else:
                order_type_str = 'UNKNOWN'
            
            # 记录成交到数据库
            trade_record = {
                'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'strategy_name': strategy,
                'stock_code': code,
                'order_type': order_type_str,
                'traded_price': filled_price,
                'traded_volume': filled_volume,
                'traded_amount': traded_amount,
                'order_id': str(order_id),
                'commission': commission
            }
            
            db_m.create_trade_record_table()
            if db_m.insert_trade_record(trade_record):
                self.logger.info(f"成交记录已保存到数据库: {trade_record}")

    def on_order_error(self, order_error):
        """委托失败推送"""
        self.logger.warning(f"委托报错回调 {order_error.order_remark} {order_error.error_msg}")

    def on_cancel_error(self, cancel_error):
        """撤单失败推送"""
        self.logger.warning(f"{sys._getframe().f_code.co_name}:cancel_error{cancel_error}")

    def on_order_stock_async_response(self, response):
        """异步下单回报推送"""
        self.logger.info(f"异步委托回调 order_remark{response.order_remark}, order_id{response.order_id}, seq{response.seq}")
        freeze_hanadler.change_seq_to_id(self.freeze_dict, response.strategy_name, response.seq, response.order_id)

    def on_cancel_order_stock_async_response(self, response):
        """异步撤单回报推送"""
        self.logger.info(f"{sys._getframe().f_code.co_name}:response{response}")

    def on_account_status(self, status):
        """账户状态推送"""
        self.logger.info(f"{sys._getframe().f_code.co_name}:status{status.status}")


# ==================== QMT交易客户端 ====================
class MyXtTrader:
    """
    QMT交易客户端封装
    
    提供交易接口的连接管理、自动重连等功能。
    """

    def __init__(self, xt_acc, path, freeze_dict):
        self._logger = logging.getLogger("MyXtTrader")
        self._logger.setLevel(logging.DEBUG)
        self._logger.addHandler(console_handler)
        self._logger.addHandler(file_handler)
        
        self._freeze_dict = freeze_dict
        self._connected = False
        self._xt_trader = None
        self._xt_acc = xt_acc
        self._path = path
        
        self._try_connect()

    def connection_lost(self):
        """标记连接断开"""
        self._connected = False
        self._xt_trader = None

    def _create_trader(self, session_id):
        """
        创建交易客户端实例
        
        :param session_id: 会话ID
        :return: 交易客户端实例或None（连接失败时）
        """
        # 创建交易回调类对象
        trader = XtQuantTrader(
            self._path, 
            session_id, 
            callback=MyXtQuantTraderCallback(
                DatabaseManager, 
                DB_NAME, 
                self, 
                self._freeze_dict
            )
        )
        
        # 开启主动请求接口的专用线程
        trader.set_relaxed_response_order_enabled(True)
        
        # 启动交易线程
        trader.start()
        
        # 建立交易连接
        connect_result = trader.connect()
        self._logger.info(f'建立交易连接，返回0表示连接成功:{connect_result}')
        
        # 订阅交易回调
        subscribe_result = trader.subscribe(self._xt_acc)
        self._logger.info(f'对交易回调进行订阅，订阅后可以收到交易主推，返回0表示订阅成功:{subscribe_result}')
        
        return trader if connect_result == 0 else None

    def _try_connect(self):
        """
        尝试连接交易接口
        
        遍历session_id列表，尝试建立连接。
        """
        session_id_range = [i for i in range(100, 120)]
        import random
        random.shuffle(session_id_range)

        for session_id in session_id_range:
            trader = self._create_trader(session_id)
            if trader:
                self._logger.info(f'连接成功，session_id:{session_id}')
                self._xt_trader = trader
                self._connected = True
                return
            else:
                self._logger.info(f'连接失败，session_id:{session_id}，继续尝试下一个id')

        self._logger.info('所有id都尝试后仍失败，放弃连接')

    def __getattr__(self, item):
        """
        代理方法调用
        
        当连接断开时自动重连。
        """
        if not self._connected:
            self._logger.info("connection lost, 交易接口断开，即将重连")
            self._try_connect()
        
        return getattr(self._xt_trader, item)


# ==================== 订单处理逻辑 ====================
def order_handle(xt_trader, freeze_dict, msg, db_m):
    """
    订单处理函数
    
    根据收到的交易信号执行下单操作。
    
    :param xt_trader: QMT交易客户端
    :param freeze_dict: 资金冻结字典
    :param msg: 交易信号消息
    :param db_m: 数据库管理器
    :return: True表示处理成功，False表示处理失败
    """
    try:
        # 获取账号信息
        account_info = xt_trader.query_stock_asset(acc)
        
        # 转换股票代码
        stock = ret_code(msg['code'])
        
        # 获取行情数据
        full_tick = xtdata.get_full_tick([stock])
        if not full_tick:
            logger.warning(f"无法获取当前{stock}行情, 下单取消!")
            return False
        
        logger.info(f"{stock} 全推行情： {full_tick}")
        current_price = full_tick[stock]['lastPrice']
        msg_price = float(msg['price'])
        logger.info(f"当前每股金额{current_price},策略信号每股金额{msg_price}")
        
        # 计算滑点
        sp = 0
        if msg_price > 0:
            sp = current_price / msg_price - 1
            logger.info(f"预计滑点{sp * 100:.3f}%")
        
        # 检查时间偏差
        info_time = msg['time']
        info_time = time.mktime(time.strptime(info_time, "%Y-%m-%d %H:%M:%S"))
        time_delay = time.time() - info_time
        logger.debug(f"下单时间与当前时间偏差：{time_delay}s")
        
        if time_check and time_delay > 1800:
            logger.error(f"时间偏差过大({time_delay}s)，可能为回测或延迟交易信号，下单取消!")
            return False

        # 处理买入信号
        if msg['action'] == 'BUY':
            # 检查滑点
            if sp > SlippagePct:
                logger.error(f'当前滑点({sp*100:.3f}%)大于设置滑点({SlippagePct*100}%)，下单取消！')
                return False
            
            # 获取可用资金
            available_cash = account_info.m_dCash
            logger.info(f'账号{acc.account_id}总可用资金{available_cash}')
            
            strategy_cash = db_m.get_available_funds(msg['strategy'])
            strategy_alv_cash = strategy_cash - freeze_hanadler.get_frozen_cash(freeze_dict, msg['strategy'])
            logger.info(f"策略{msg['strategy']}总现金{strategy_cash}，可用资金{strategy_alv_cash}")
            
            # 计算买入金额和数量
            buy_amount = min(available_cash, strategy_alv_cash) * float(msg['pct'])
            buy_vol = int(buy_amount / current_price / 100) * 100
            logger.info(f"策略可用资金{strategy_alv_cash}；账号可用资金 {available_cash}；目标买入金额 {buy_amount}；买入股数 {buy_vol}股")
            
            if buy_vol > 0:
                async_seq = xt_trader.order_stock_async(
                    acc, 
                    stock, 
                    xtconstant.STOCK_BUY, 
                    buy_vol,
                    xtconstant.MARKET_PEER_PRICE_FIRST,
                    0,
                    msg['strategy'], 
                    stock
                )
                
                if async_seq > 0:
                    logger.debug(f"下单序号{async_seq}")
                    freeze_hanadler.freeze_cash(freeze_dict, msg['strategy'], async_seq, buy_vol, current_price)
                    logger.info(f"买入下单完成 等待回调")
                    return True
                else:
                    logger.error("买入下单失败")
                    return False
            else:
                logger.warning(f"可用资金不足，下单取消！")
                return False

        # 处理卖出信号
        elif msg['action'] == 'SELL':
            # 获取持仓信息
            positions = xt_trader.query_stock_positions(acc)
            position_total_dict = {i.stock_code: i.m_nVolume for i in positions}
            position_available_dict = {i.stock_code: i.m_nCanUseVolume for i in positions}
            
            logger.info(f'{acc.account_id}持仓字典{position_total_dict}')
            logger.info(f'{acc.account_id}可用持仓字典{position_available_dict}')
            
            # 计算卖出数量
            available_vol = position_available_dict.get(stock, 0)
            strategy_vol = db_m.get_position(msg['strategy'], stock)
            target_vol = int(strategy_vol * float(msg['pct']) / 100) * 100
            sell_vol = min(target_vol, available_vol)
            
            logger.info(f"策略现有股数{strategy_vol}；{stock} 目标卖出量 {target_vol} 可用数量 {available_vol} 卖出 {sell_vol}股")
            
            if sell_vol > 0:
                async_seq = xt_trader.order_stock_async(
                    acc, 
                    stock, 
                    xtconstant.STOCK_SELL, 
                    sell_vol,
                    xtconstant.MARKET_PEER_PRICE_FIRST,
                    0,
                    msg['strategy'], 
                    stock
                )
                
                if async_seq > 0:
                    logger.debug(f"下单序号{async_seq}")
                    freeze_hanadler.pre_add_cash(freeze_dict, msg['strategy'], async_seq, sell_vol, current_price)
                    logger.info(f"卖出下单完成 等待回调")
                    return True
                else:
                    logger.error("卖出下单失败")
                    return False

        else:
            logger.info(f"错误的action：{msg['action']}")
            return False
    
    except Exception as e:
        logger.error(f"订单处理异常: {traceback.format_exc()}")
        return False


def ret_code(stock_code):
    """
    将股票代码转换为QMT格式
    
    :param stock_code: 原始股票代码
    :return: QMT格式的股票代码
    """
    if stock_code[:2] in ["60", "68", "90", "50", "51"]:
        return stock_code[:6] + ".SH"
    elif stock_code[:2] in ["00", "30", "15"] or "XSHG" in stock_code:
        return stock_code[:6] + ".SZ"
    elif stock_code[:2] in ["43", "83", "87"]:
        return stock_code[:6] + ".BJ"
    elif "XSHG" in stock_code:
        return stock_code[:6] + ".SH"
    elif "XSHE" in stock_code:
        return stock_code[:6] + ".SZ"
    else:
        return stock_code


# ==================== 主函数 ====================
if __name__ == '__main__':
    logger.info("========== 启动量化交易信号接收服务 ==========")
    
    # 初始化数据库管理器
    with DatabaseManager(DB_NAME) as db_manager:
        strategy_names = db_manager.get_all_strategy_names()
    
    logger.info(f"监听中的策略：{strategy_names}")
    
    # 初始化Stream追踪字典
    stream_dict = {name: '$' for name in strategy_names}

    with Manager() as p_manager:
        # 创建进程间共享的冻结字典
        freeze_dict = p_manager.dict()

        # 创建QMT交易客户端
        my_trader = MyXtTrader(acc, path, freeze_dict)

        # 连接到Redis服务器
        redis_client = redis.StrictRedis(
            host='***.redis-***',  # 替换为实际的Redis地址
            port=10973,            # 替换为实际的Redis端口
            password='******',     # 替换为实际的Redis密码
            decode_responses=True
        )

        # 创建消息处理器和追踪器（优化后的核心组件）
        message_processor = MessageProcessor(redis_client)
        message_tracker = MessageTracker(redis_client)

        logger.info("========== 开始从Redis流中读取订单信号 ==========")
        last_time = time.time()

        while True:
            # 从Redis Stream读取消息
            response = redis_client.xread(stream_dict, block=100)

            now_time = time.time()
            timedelta = now_time - last_time
            last_time = now_time
            
            # 打印心跳指示
            print('.', end="", flush=True)
            
            # 检测长时间无消息情况
            if timedelta > 3:
                logger.warning(f"长时间无消息: {timedelta:.2f}s")
            
            # 处理收到的消息
            if response:
                logger.info(f"\n收到消息，时间间隔: {timedelta:.2f}s")
                logger.info(f"消息数量: {len(response)}")
                
                for stream, messages in response:
                    logger.info(f"Stream: {stream}, 消息数: {len(messages)}")
                    
                    for msg_id, order_data in messages:
                        logger.info(f"处理消息 - Stream:{stream}, ID:{msg_id}, 数据:{order_data}")
                        
                        # 使用消息处理器处理消息（实现幂等性和消息确认）
                        def business_handler(msg_data):
                            """业务处理函数包装"""
                            with DatabaseManager(DB_NAME) as db_m:
                                return order_handle(my_trader, freeze_dict, msg_data, db_m)
                        
                        # 处理消息
                        success = message_processor.process_message(
                            stream_name=stream,
                            msg_id=msg_id,
                            msg_data=order_data,
                            handler=business_handler
                        )
                        
                        # 记录消息跟踪状态
                        if success:
                            message_tracker.track_message(msg_id, 'success', {
                                'stream': stream,
                                'strategy': order_data.get('strategy')
                            })
                        else:
                            message_tracker.track_message(msg_id, 'failed', {
                                'stream': stream,
                                'strategy': order_data.get('strategy')
                            })
                        
                        # 更新Stream追踪ID
                        stream_dict[stream] = msg_id