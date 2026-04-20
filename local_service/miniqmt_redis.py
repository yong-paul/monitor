# coding:utf-8
import time, datetime, traceback, sys
import logging
import redis
from multiprocessing import Manager

from xtquant import xtdata, xttype
from xtquant.xttrader import XtQuantTrader, XtQuantTraderCallback
from xtquant.xttype import StockAccount
from xtquant import xtconstant

from db_manager import DatabaseManager


"""
通过Redis的Stream模式获取交易信号，并驱动miniQMT下单

主要特性：
1.  接收聚宽发送的百分比下单信号，并根据本地数据库中的资金量进行下单，策略实际资金可与聚宽不同，本地数据库中的数据也方便自行调整。
2.  可同时监听多个策略的信号并下单，策略之间资金互相独立。
3.  Stream模式获取交易信号可避免网络等原因导致的信号丢失。
4.  加入miniQMT的交易接口断开时自动重连逻辑。
5.  下单时加入了时间差检查和滑点检查。

注意：
    1) 使用前将redis.StrictRedis方法中的host,port,password以及miniQMT的账号信息改成自己的
    2) 下单依赖本地数据库，请使用db_manager.py文件中的create_strategy_table方法创建本地数据库和策略表，仅监听和操作本地数据库中存在的策略
    3) 为保证成交，委托均为对手方最优价的市价委托（实际委托以涨停或跌停价格发布），可根据实际情况自行修改。
"""

DB_NAME = 'strategy_data.db'
acc = StockAccount('your_account_id', 'STOCK')  # 填写你自己的账号id信息
path = r'D:\国金QMT交易端模拟\userdata_mini' # 填写你自己的userdata_mini路径

time_check = True   # 是否检查下单时间差
SlippagePct = 0.01  # 买入时允许下单的滑点阈值


console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter(fmt="%(asctime)s.%(msecs)03d - %(name)s - %(levelname)s - %(message)s",
                              datefmt="%Y-%m-%d %H:%M:%S")
console_handler.setFormatter(formatter)
nowtime = time.strftime("%Y%m%d_%H%M%S")
logpath = fr".\log\qmt_{nowtime}.log"
file_handler = logging.FileHandler(logpath, mode="a", encoding="utf-8")
file_handler.setFormatter(formatter)
logger = logging.getLogger("joinquant_to_qmt")
logger.setLevel(logging.DEBUG)
logger.addHandler(console_handler)
logger.addHandler(file_handler)

# 定义一个类 创建类的实例 作为状态的容器
class _a():
    pass


A = _a()
A.bought_list = []
A.hsa = xtdata.get_stock_list_in_sector('沪深A股')


def interact():
    """执行后进入repl模式"""
    import code
    code.InteractiveConsole(locals=globals()).interact()


# xtdata.download_sector_data()


class freeze_hanadler:

    @staticmethod
    def pre_add_cash(freeze_dict, strategy_name, order_seq, vol, price):
        ca = vol * price
        logger.debug(f"预存入策略{strategy_name}资金{vol}*{price}={ca}，order_seq：{order_seq}")
        strategy_dict = freeze_dict.setdefault(strategy_name, p_manager.dict())
        strategy_dict[f"seq{order_seq}"] = [-vol, price]

    @staticmethod
    def freeze_cash(freeze_dict, strategy_name, order_seq, vol, price):
        ca = vol * price
        logger.debug(f"冻结策略{strategy_name}资金{vol}*{price}={ca}，order_seq：{order_seq}")
        strategy_dict = freeze_dict.setdefault(strategy_name, p_manager.dict())
        strategy_dict[f"seq{order_seq}"] = [vol, price]

    @staticmethod
    def unfreeze_cash(freeze_dict, strategy_name, order_id, vol_change):
        strategy_dict = freeze_dict.get(strategy_name)
        logger.debug(f"当前冻结字典：{freeze_dict}，策略{strategy_name}冻结字典：{strategy_dict}，成交order_id：{order_id}")
        if strategy_dict:
            vol, price = strategy_dict.get(f"ord{order_id}")
            # vol_change买入为正，卖出为负
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

    @staticmethod
    def change_seq_to_id(freeze_dict, strategy_name, order_seq, order_id):
        strategy_dict = freeze_dict.get(strategy_name)
        logger.debug(f"策略{strategy_name}冻结字典:{strategy_dict}，order_seq:{order_seq}, order_id:{order_id}")
        if strategy_dict:
            strategy_dict[f"ord{order_id}"] = strategy_dict.pop(f"seq{order_seq}")

    @staticmethod
    def get_frozen_cash(freeze_dict, strategy_name):
        frozen_cash = 0
        strategy_dict = freeze_dict.get(strategy_name)
        if strategy_dict:
            for vol, price in strategy_dict.values():
                frozen_cash += vol * price
        logger.debug(f"策略总计冻结资金{frozen_cash}")
        return frozen_cash



class MyXtQuantTraderCallback(XtQuantTraderCallback):

    def __init__(self, manager, db_name, xt_trader, freeze_dict):
        self.logger = logging.getLogger("MyXtQuantTraderCallback")
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)
        self.db_manager = manager
        self.db_name = db_name
        self.xt_trader = xt_trader
        self.freeze_dict = freeze_dict
        # self.call_back_num = 0

    def on_disconnected(self):
        """
        连接断开
        :return:
        """
        self.logger.warning("连接断开回调: connection lost, 交易接口断开")
        self.xt_trader.connection_lost()

    def on_stock_order(self, order):
        """
        委托回报推送
        :param order: XtOrder对象
        :return:
        """
        self.logger.info(f'委托回调:{order.order_remark}')

    def on_stock_trade(self, trade: xttype.XtTrade):
        """
        成交变动推送
        :param trade: XtTrade对象
        :return:
        """
        self.logger.info(f'成交回调: {trade.order_remark}')
        with self.db_manager(self.db_name) as db_m:
            strategy = trade.strategy_name
            code = trade.stock_code
            filled_price = trade.traded_price
            filled_volume = trade.traded_volume
            traded_amount = trade.traded_amount
            action = trade.order_type  # 交易方向: 买入或卖出
            order_id = trade.order_id
            commission = trade.commission
            self.logger.info(f"成交详情：strategy:{strategy},code:{code},filled_price:{filled_price},"
                             f"filled_volume:{filled_volume},traded_amount:{traded_amount},action:{action},"
                             f"order_id:{order_id},commission:{commission}")
            self.logger.debug(f"当前冻结字典：{self.freeze_dict[strategy]}")
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
            import datetime
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
            # 确保成交记录表存在
            db_m.create_trade_record_table()
            # 插入成交记录
            if db_m.insert_trade_record(trade_record):
                self.logger.info(f"成交记录已保存到数据库: {trade_record}")


    def on_order_error(self, order_error):
        """
        委托失败推送
        :param order_error:XtOrderError 对象
        :return:
        """

        self.logger.warning(f"委托报错回调 {order_error.order_remark} {order_error.error_msg}")

    def on_cancel_error(self, cancel_error):
        """
        撤单失败推送
        :param cancel_error: XtCancelError 对象
        :return:
        """
        self.logger.warning(f"{sys._getframe().f_code.co_name}:cancel_error{cancel_error}" )

    def on_order_stock_async_response(self, response):
        """
        异步下单回报推送
        :param response: XtOrderResponse 对象
        :return:
        """
        self.logger.info(f"异步委托回调 order_remark{response.order_remark}, order_id{response.order_id}, seq{response.seq}")
        self.logger.debug(f"当前冻结字典：{self.freeze_dict[response.strategy_name]}")
        freeze_hanadler.change_seq_to_id(self.freeze_dict, response.strategy_name,response.seq, response.order_id)

    def on_cancel_order_stock_async_response(self, response):
        """
        :param response: XtCancelOrderResponse 对象
        :return:
        """
        self.logger.info(f"{sys._getframe().f_code.co_name}:response{response}" )

    def on_account_status(self, status):
        """
        :param response: XtAccountStatus 对象
        :return:
        """
        self.logger.info(f"{sys._getframe().f_code.co_name}:status{status.status}")


class MyXtTrader:

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
        self._connected = False
        self._xt_trader = None

    def _create_trader(self, session_id):
        # 创建交易回调类对象，并声明接收回调
        trader = XtQuantTrader(self._path, session_id, callback=MyXtQuantTraderCallback(DatabaseManager, DB_NAME, self, self._freeze_dict))
        # 开启主动请求接口的专用线程 开启后在on_stock_xxx回调函数里调用XtQuantTrader.query_xxx函数不会卡住回调线程，但是查询和推送的数据在时序上会变得不确定
        # 详见: http://docs.thinktrader.net/vip/pages/ee0e9b/#开启主动请求接口的专用线程
        trader.set_relaxed_response_order_enabled(True)
        # 启动交易线程
        trader.start()
        connect_result = trader.connect()
        self._logger.info(f'建立交易连接，返回0表示连接成功:{connect_result}', )
        # 对交易回调进行订阅，订阅后可以收到交易主推，返回0表示订阅成功
        subscribe_result = trader.subscribe(self._xt_acc)
        self._logger.info(f'对交易回调进行订阅，订阅后可以收到交易主推，返回0表示订阅成功:{subscribe_result}', )
        return trader if connect_result == 0 else None

    def _try_connect(self):
        session_id_range = [i for i in range(100, 120)]

        import random
        random.shuffle(session_id_range)

        # 遍历尝试session_id列表尝试连接
        for session_id in session_id_range:
            trader = self._create_trader(session_id)
            if trader:
                self._logger.info(f'连接成功，session_id:{session_id}')
                self._xt_trader = trader
                self._connected = True
                return
            else:
                self._logger.info(f'连接失败，session_id:{session_id}，继续尝试下一个id')
                continue

        self._logger.info('所有id都尝试后仍失败，放弃连接')
        # return None

    def __getattr__(self, item):
        if not self._connected:
            self._logger.info("connection lost, 交易接口断开，即将重连")
            self._try_connect()
        return getattr(self._xt_trader, item)



def order_handle(xt_trader, freeze_dict, msg, db_m):
    # 请在此处自己coding, 根据msg给交易端下单
    # {'time': '2019-01-02 09:38:00', 'action': 'BUY', 'code': '511010.XSHG',
    # 'pct': 0.0116, 'strategy': 'my_test', 'price': 116.409, 'cancel_order': 0}
    # logger.info(f"下单信息：{msg}")
    #取账号信息
    account_info = xt_trader.query_stock_asset(acc)
    # logger.info(account_info)
    stock = ret_code(msg['code'])
    full_tick = xtdata.get_full_tick([stock])
    if not full_tick:
        logger.warning(f"无法获取当前{stock}行情, 下单取消!")
        return
    logger.info(f"{stock} 全推行情： {full_tick}")
    current_price = full_tick[stock]['lastPrice']
    msg_price = float(msg['price'])
    logger.info(f"当前每股金额{current_price},策略信号每股金额{msg_price}")
    sp = 0
    if msg_price > 0:
        sp = current_price / msg_price - 1
        logger.info(f"预计滑点{sp * 100:.3f}%")
    info_time = msg['time']
    info_time = time.mktime(time.strptime(info_time, "%Y-%m-%d %H:%M:%S"))
    time_delay = time.time() - info_time
    logger.debug(f"下单时间与当前时间偏差：{time_delay}s")
    if time_check and time_delay > 1800:  # 允许最多30分钟下单时间偏差，防止延迟交易信号异常发送
        logger.error(f"时间偏差过大，可能为回测或延迟交易信号，下单取消!")
        return

    if msg['action'] == 'BUY':
        if sp > SlippagePct:  # 仅在买入时检查滑点
            logger.error(f'当前滑点大于设置滑点，下单取消！')
            return
        # 取可用资金
        available_cash = account_info.m_dCash
        logger.info(f'账号{acc.account_id}总可用资金{available_cash}')
        strategy_cash = db_m.get_available_funds(msg['strategy'])
        strategy_alv_cash = strategy_cash - freeze_hanadler.get_frozen_cash(freeze_dict,msg['strategy'])
        logger.info(f"策略{msg['strategy']}总现金{strategy_cash}，可用资金{strategy_alv_cash}")
        # 买入金额
        buy_amount = min(available_cash, strategy_alv_cash) * float(msg['pct'])
        # 买入数量 取整为100的整数倍
        buy_vol = int(buy_amount / current_price / 100) * 100
        logger.info(f"策略可用资金{strategy_alv_cash}；账号可用资金 {available_cash}；目标买入金额 {buy_amount}；买入股数 {buy_vol}股")
        if buy_vol > 0:
            async_seq = xt_trader.order_stock_async(acc, stock, xtconstant.STOCK_BUY, buy_vol,
                                                    xtconstant.MARKET_PEER_PRICE_FIRST,
                                                    0,
                                                    msg['strategy'], stock)
            if async_seq > 0:
                logger.debug(f"下单序号{async_seq}")
                freeze_hanadler.freeze_cash(freeze_dict, msg['strategy'], async_seq, buy_vol, current_price)
                logger.info(f"买入下单完成 等待回调")
            else:
                logger.error("买入下单失败")
        else:
            logger.warning(f"可用资金不足，下单取消！")

    elif msg['action'] == 'SELL':
        # 查账号持仓
        positions = xt_trader.query_stock_positions(acc)
        # 取各品种 总持仓 可用持仓
        position_total_dict = {i.stock_code: i.m_nVolume for i in positions}
        position_available_dict = {i.stock_code: i.m_nCanUseVolume for i in positions}
        logger.info(f'{acc.account_id}持仓字典{position_total_dict}')
        logger.info(f'{acc.account_id}可用持仓字典{position_available_dict}')

        # 可用数量
        available_vol = position_available_dict[stock] if stock in position_available_dict else 0
        # 策略现有数量
        strategy_vol = db_m.get_position(msg['strategy'], stock)
        # 目标卖出数量
        target_vol = int(strategy_vol * float(msg['pct']) / 100) * 100
        # 卖出量取目标量与可用量中较小的
        sell_vol = min(target_vol, available_vol)
        logger.info(f"策略现有股数{strategy_vol}；{stock} 目标卖出量 {target_vol} 可用数量 {available_vol} 卖出 {sell_vol}股")
        if sell_vol > 0:
            async_seq = xt_trader.order_stock_async(acc, stock, xtconstant.STOCK_SELL, sell_vol,
                                                    xtconstant.MARKET_PEER_PRICE_FIRST,
                                                    0,
                                                    msg['strategy'], stock)
            if async_seq > 0:
                logger.debug(f"下单序号{async_seq}")
                freeze_hanadler.pre_add_cash(freeze_dict, msg['strategy'], async_seq, sell_vol, current_price)
                logger.info(f"卖出下单完成 等待回调")
            else:
                logger.error("卖出下单失败")

    else:
        logger.info(f"错误的action：{msg['action']}")


def ret_code(stock_code):  # 将收到的股票代码进行转换   含基金
    if stock_code[:2] in ["60", "68", "90", "50", "51"]:
        return stock_code[:6] + ".SH"
    elif stock_code[:2] in ["00", "30", "15"] or "XSHG" in stock_code:  #
        return stock_code[:6] + ".SZ"
    elif stock_code[:2] in ["43", "83", "87"]:
        return stock_code[:6] + ".BJ"
    elif "XSHG" in stock_code:
        return stock_code[:6] + ".SH"
    elif "XSHE" in stock_code:
        return stock_code[:6] + ".SZ"
    else:
        return stock_code


if __name__ == '__main__':

    logger.info("start")
    # 初始化数据库管理器
    with DatabaseManager(DB_NAME) as db_manager:
        strategy_names = db_manager.get_all_strategy_names()
    logger.info(f"监听中的策略：{strategy_names}")
    stream_dict = {}
    for name in strategy_names:
        stream_dict[name] = '$' # 初始ID 0-0


    with Manager() as p_manager:
        freeze_dict = p_manager.dict()

        my_trader = MyXtTrader(acc, path, freeze_dict)

        # 连接到 Redis 服务器, 改为你自己的域名和密码
        redis_client = redis.StrictRedis(host='***.redis-***', port=10973,
                                         password='******', decode_responses=True)

        logger.info("从Redis流中读取最新的订单信号")
        last_time = time.time()
        # freeze_info.freeze_cash("my_t", 10, 12345)

        while True:
            # 从Redis流中读取最新的订单信号
            response = redis_client.xread(stream_dict, block=100)

            now_time = time.time()
            timedelta = now_time-last_time
            last_time = now_time
            print('.', end="")
            # print(round(timedelta*1000), end=" ")
            if timedelta > 3:
                logger.warning(f"long timedelta:{timedelta}")
            if response:
                logger.info(f"timedelta:{timedelta}")
                logger.info(f"len_response:{len(response)}")
                for stream, messages in response:
                    del_ids = []
                    logger.info(f"len_messages:{len(messages)}")
                    for msg_id, order_data in messages:
                        # order_data = json.loads(order_data)
                        logger.info(f"stream:{stream}, msg_id:{msg_id}, 下单信息:{order_data}")
                        with DatabaseManager(DB_NAME) as db_manager:
                            order_handle(my_trader, freeze_dict, order_data, db_manager)
                        # 更新 last_id 并删除处理过的消息
                        stream_dict[stream] = msg_id
                        # redis_client.xdel(stream, msg_id)
                        del_ids.append(msg_id)
                        # logger.info(f"Outer call_back_num: {callback.call_back_num}, ID:{id(callback.call_back_num)}")
                    if del_ids:
                        redis_client.xdel(stream, *del_ids)


