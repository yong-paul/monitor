import redis
import json
import time
from functools import wraps
from kuanke.user_space_api import *

"""
通过Redis的Stream模式传递交易信号

注意：
    1)使用前将RedisTrade类中的host,port, password改成自己的
    2)选择Redis模式：pattern=0，pubsub模式；pattern=1，stream模式
    3)选择策略mode：mode = 0, 测试：策略历史回测(back_test)时，发送Redis信号；
                  mode = 1, 正式：策略模拟交易(sim_trade)时，发送Redis信号。
"""

__version__ = '20250114'

class RedisTrade:
    host = '8.153.77.252' # 此处填写你自己服务器的域名或ip
    port = 6378
    password = '7712'   # 此处填写你自己服务器的密码
    pattern = 1  # 0:PUBSUB模式，1：STREAM模式
    mode = 0  # 0: 测试，1：正式

    @staticmethod
    def trade_signal(func):

        @wraps(func)
        def wrapper(*args, **kwargs):
            context = kwargs.get('context') or args[0]
            security = args[1].security if len(args) == 2 else kwargs.get('security') or args[1]
            # 下单前的现金、股票数量
            pre_cash = context.portfolio.available_cash
            pre_amt = 0
            if security in context.portfolio.positions:
                pre_amt = context.portfolio.positions[security].total_amount
            my_order = func(*args, **kwargs)
            if my_order is not None:
                order_amt = my_order.amount
                limit_stings = str(my_order.style)
                p1 = limit_stings.find('=')
                limit_price = float(limit_stings[p1 + 1:])
                order_price = max(my_order.price, limit_price)
                cedan_status = str(my_order.status)
                cedan = 1 if cedan_status == 'canceled' else 0
                if my_order.is_buy:  # 买入，看现金
                    new_cash = context.portfolio.available_cash  # 10W
                    order_amount = pre_cash - new_cash
                    pct = round(order_amount / pre_cash, 8)  # 计算本次实际使用现金占下单前总现金的比例
                else:  # 卖出，看持仓
                    pct = round(order_amt / pre_amt, 8)  # 1-500/2000 = 3/4, 即卖出现持仓的股票的3/4
                    # if security not in context.portfolio.positions:
                    #     new_amt = 0
                    # else:
                    #     new_amt = context.portfolio.positions[security].total_amount  # 卖出后，持有的数量500股
                    # pct = round(1.0 - new_amt / pre_amt, 8)  # 1-500/2000 = 3/4, 即卖出现持仓的股票的3/4
                #
                data = {
                    'time': my_order.add_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'action': 'BUY' if my_order.is_buy else 'SELL',
                    'code': security,
                    'pct': pct,
                    'strategy': g.strategy,
                    'price': order_price,
                    'cancel_order': cedan
                }

                log.info(data)

                if context.run_params.type == 'sim_trade' or RedisTrade.mode == 0:  # mode = 0: 测试； mode == 1：正式
                    try:
                        rds = RedisTrade._open()
                        if RedisTrade.pattern == 0:  # 0:PUBSUB模式，1：STREAM模式
                            rds.publish(g.strategy, json.dumps(data))
                        else:
                            rds.xadd(g.strategy, data, maxlen=200)
                    except Exception as e:
                        log.error(repr(e))

            return my_order

        return wrapper

    @staticmethod
    def _open():
        if hasattr(g, 'rds_connected') and g.rds_connected:
            rds = g.__dict__.get('__redis')
            if rds:
                return rds

        pool = redis.ConnectionPool(
            host=RedisTrade.host,
            port=RedisTrade.port,
            password=RedisTrade.password)
        rds = redis.Redis(connection_pool=pool)
        rds.auto_close_connection_pool = True

        g.__dict__.update({'__redis': rds})
        g.rds_connected = True

        return rds

    @staticmethod
    def close():
        if hasattr(g, 'rds_connected') and (not g.rds_connected):
            return
        try:
            rds = g.__dict__.get('__redis')  # type: redis.Redis
            g.__dict__.update({'__redis': None})
            if rds:
                rds.connection_pool.disconnect()
        except Exception as e:
            log.error(repr(e))
        finally:
            g.rds_connected = False


@RedisTrade.trade_signal
def order_(context, *args, **kwargs):
    _order = order(*args, **kwargs)
    return _order


@RedisTrade.trade_signal
def order_target_(context, *args, **kwargs):
    _order = order_target(*args, **kwargs)
    return _order


@RedisTrade.trade_signal
def order_value_(context, *args, **kwargs):
    _order = order_value(*args, **kwargs)
    return _order


@RedisTrade.trade_signal
def order_target_value_(context, *args, **kwargs):
    _order = order_target_value(*args, **kwargs)
    return _order


@RedisTrade.trade_signal
def cancel_order_(context, orderid):
    _order = cancel_order(orderid)
    # time.sleep(3)
    return _order
