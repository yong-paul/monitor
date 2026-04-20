# 克隆自聚宽文章：https://www.joinquant.com/post/69762

from jqdata import *
from redistrade_sim import *
def initialize(context):
    # 设置日志级别
    log.set_level('system', 'error')
    # 避免使用未来数据（聚宽平台级未来数据防护）
    set_option("avoid_future_data", True)
    # 设定沪深300作为基准
    set_benchmark('000300.XSHG')
    # 开启动态复权模式（真实价格）
    set_option('use_real_price', True)
    # 设置交易成本（针对基金/ETF）
    set_order_cost(
        OrderCost(close_tax=0.000, open_commission=0.00025, close_commission=0.00025, min_commission=5), 
        type='fund'
    )
    g.strategy = 'ETF_OverRate'
    # 设置滑点（固定滑点0.1%）
    set_slippage(FixedSlippage(0.001))
    
    # 每日运行函数调度
    run_daily(before_market_open, '09:20')
    run_daily(market_open, '09:30:03')
    run_daily(handle_risk_management, '09:30:05')
    run_daily(handle_risk_management, '14:55')
     # 收盘后运行
    run_daily(after_market_close, time='after_close', reference_security='000300.XSHG')

def before_market_open(context):
    """开盘前预处理：获取前一日ETF数据"""
    try:
        fund_list = get_all_securities(['etf'], context.previous_date).index.tolist()

        high_df = history(count=1, unit='1d', field="high", security_list=fund_list).T
        low_df = history(count=1, unit='1d', field="low", security_list=fund_list).T
        volume_df = history(count=1, unit='1d', field="money", security_list=fund_list).T

        df = high_df.merge(low_df, left_index=True, right_index=True)
        df = df.merge(volume_df, left_index=True, right_index=True)
        df.columns = ['high_price', 'low_price', 'money']

        df = df[(df['money'] < 1e8) & (df['money'] > 5e7)]

        nav_df = get_extras(
            'unit_net_value', 
            df.index.tolist(), 
            end_date=context.previous_date, 
            df=True, 
            count=1
        ).T
        nav_df.columns = ['unit_net_value']

        g.fund_list = df.merge(nav_df, left_index=True, right_index=True)
        
    except Exception as e:
        log.error(f"开盘前预处理异常：{e}")
        g.fund_list = None

def market_open(context):
    """开盘执行：选股+按折价率权重下单"""
    try:
        if g.fund_list is None or g.fund_list.empty:
            log.warning("无符合条件的ETF标的，跳过下单")
            return

        df = g.fund_list.copy()
        current_data = get_current_data()

        df['last_price'] = [current_data[code].last_price for code in df.index.tolist()]
        df['premium'] = (df['last_price'] / df['unit_net_value'] - 1) * 100

        df = df[df['premium'] < 0].sort_values(['premium'], ascending=True)
        selected_funds = df.head(3)
        order_fund_codes = selected_funds.index.tolist()

        log.info(f"选中的ETF标的：{order_fund_codes}")

        # 清仓不在目标列表的
        for hold_code in context.portfolio.positions:
            if hold_code not in order_fund_codes:
                order_target_(context, hold_code, 0, style=MarketOrderStyle())

        # 加权买入
        if not selected_funds.empty:
            weights = selected_funds['premium'].abs().tolist()
            total_weight = sum(weights) if sum(weights) != 0 else 1e-9
            available_cash = context.portfolio.available_cash

            for code, weight in zip(order_fund_codes, weights):
                target_value = available_cash * (weight / total_weight)
                order_target_value_(context, code, target_value, style=MarketOrderStyle())

    except Exception as e:
        log.error(f"开盘下单异常：{e}")

def handle_risk_management(context):
    """风控：止盈10% 止损5%"""
    try:
        # 【已修复】去掉错误的 trading_state
        for hold_code in context.portfolio.positions:
            position = context.portfolio.positions[hold_code]
            current_price = position.price
            cost_basis = position.avg_cost

            if current_price < cost_basis * 0.95:
                log.info(f"止损触发：{hold_code} 成本{cost_basis:.3f} 当前{current_price:.3f}")
                order_target_(context,hold_code, 0, style=MarketOrderStyle())

            elif current_price > cost_basis * 1.10:
                log.info(f"止盈触发：{hold_code} 成本{cost_basis:.3f} 当前{current_price:.3f}")
                order_target_(context,hold_code, 0, style=MarketOrderStyle())
                
    except Exception as e:
        log.error(f"风控执行异常：{e}")
        
def after_market_close(context):
    RedisTrade.close()