# -*- coding: utf-8 -*-
"""
统一配置文件
所有可修改参数都在这里，无需改动业务代码
"""

# ====================== Redis 配置 ======================
REDIS_HOST = "8.153.77.252"
REDIS_PORT = 6378
REDIS_PASSWORD = "7712"

# ====================== 心跳 & 上报配置 ======================
# 本地监控服务心跳频率（秒）
HEARTBEAT_INTERVAL = 30

# 本地监控完整上报云端频率（秒）→ 5分钟（心跳每次都上报）
REPORT_INTERVAL = 300

# 心跳超时判断（秒）→ 设置为心跳间隔的 2 倍
HEARTBEAT_TIMEOUT = 60

# ====================== 本地监控专用配置 ======================
# 本地Redis（一般不用改）
LOCAL_REDIS_HOST = "localhost"
LOCAL_REDIS_PORT = 6379
LOCAL_REDIS_PASSWORD = None

# MiniQMT 路径
MINIQMT_PATH = r"D:\国金QMT交易端模拟\userdata_mini"

# ====================== Flask 网页配置 ======================
FLASK_HOST = "0.0.0.0"
FLASK_PORT = 5000
FLASK_DEBUG = True

# 页面自动刷新时间（毫秒）→ 5分钟
WEB_REFRESH_INTERVAL = 300000