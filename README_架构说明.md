
# 量化交易系统 - 架构说明

## 系统架构概览

```
┌─────────────────────────────────────────────────────────────────┐
│                        聚宽平台 (云端)                          │
│  ┌──────────────┐    ┌──────────────────┐                    │
│  │  策略运行    │───▶│  Redis信号推送   │                    │
│  └──────────────┘    └────────┬─────────┘                    │
└────────────────────────────────┼───────────────────────────────┘
                                 │
                                 │ Stream 推送
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                      云服务器 (自购已有)                        │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                Redis 服务 (核心枢纽)                       │  │
│  │  • 接收聚宽策略信号 (Stream)                               │  │
│  │  • 存储本地监控状态 (monitor:status)                       │  │
│  │  • 存储监控历史 (monitor:history)                          │  │
│  └──────────────────────────────────────────────────────────┘  │
│                              ▲                                  │
│                              │ 上报监控数据                    │
│  ┌─────────────────────────┼───────────────────────────────┐  │
│  │                         │                               │  │
│  │  ┌──────────────────────┴───────────────────────────┐  │  │
│  │  │         cloud_service (可视化展示)                │  │  │
│  │  │  • Flask Web 应用                                │  │  │
│  │  │  • 从 Redis 读取数据                             │  │  │
│  │  │  • 提供 Web 监控面板供任意浏览器访问              │  │  │
│  │  └──────────────────────────────────────────────────┘  │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                                 ▲
                                 │ 数据上报
                                 │
┌─────────────────────────────────────────────────────────────────┐
│                     本地机器 (笔记本)                           │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  monitor_service (监控服务)                              │  │
│  │  • 监控: 本地 Redis 连通性                              │  │
│  │  • 监控: Redis信号接收进程 (miniqmt_redis.py)          │  │
│  │  • 监控: MiniQMT 进程运行状态                           │  │
│  │  • 监控: 系统资源 (CPU/内存/磁盘)                       │  │
│  │  • 上报: 状态数据到云端 Redis                            │  │
│  └──────────────────────────────────────────────────────────┘  │
│                              │                                  │
│                              ▼                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  miniqmt_redis.py (信号接收与下单)                        │  │
│  │  • 从云端 Redis 接收交易信号                             │  │
│  │  • 通过 xtquant API 调用 MiniQMT 下单                    │  │
│  │  • 成交回报自动落库 (trade_records 表)                   │  │
│  └──────────────────────────────────────────────────────────┘  │
│                              │                                  │
│                              ▼                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  db_manager.py + SQLite 数据库                            │  │
│  │  • strategy_data.db                                       │  │
│  │  • 各策略持仓表 (如 g9small)                             │  │
│  │  • trade_records 表 (成交记录总表)                       │  │
│  └──────────────────────────────────────────────────────────┘  │
│                              │                                  │
│                              ▼                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                    MiniQMT (下单执行)                      │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## 数据流程详解

### 1. 本地监控数据上报流程

```
本地电脑 (monitor_service.py)
    ↓ 每 5 秒检查一次
    ├─ 检查本地 Redis 连接
    ├─ 检查 miniqmt_redis.py 进程是否运行
    ├─ 检查 MiniQMT 进程是否运行
    └─ 检查系统资源 (CPU/内存/磁盘)
    ↓
将状态数据上报到云端 Redis
    ↓ 存储到 Key: monitor:status
云端 Redis
    ↓
cloud_service (app.py) 读取
    ↓ /api/monitor 接口
Web 页面展示 (index.html)
    ↓
任意浏览器访问 http://云服务器IP:5000
```

### 2. 交易信号流程

```
聚宽平台 (strategy.py)
    ↓ 生成买卖信号
推送到云端 Redis Stream
    ↓
云端 Redis
    ↓
本地机器 (miniqmt_redis.py) 读取
    ↓
通过 xtquant API 调用 MiniQMT 下单
    ↓
MiniQMT 执行交易
    ↓
成交回报回调
    ↓
更新策略持仓表 + 记录到 trade_records 表
    ↓
SQLite 数据库 (strategy_data.db)
```

## 各模块说明

### cloud_service (云服务器)

**文件位置**: `cloud_service/`

**功能**:
- `app.py` - Flask Web 应用主程序
- `templates/index.html` - 监控面板页面
- `templates/simple.html` - 简单测试页面
- `requirements.txt` - Python 依赖

**API 接口**:
- `GET /` - 主页（监控面板）
- `GET /simple` - 简单测试页面
- `GET /api/status` - 获取云端 Redis 和策略流状态
- `GET /api/monitor` - 获取本地监控服务上报的状态
- `GET /api/trades` - 获取最近交易记录
- `GET /api/streams/&lt;stream_name&gt;` - 获取指定 Stream 的数据

**配置修改**:
修改 `app.py` 中的 Redis 配置：
```python
REDIS_HOST = '您的云端Redis地址'
REDIS_PORT = 10973
REDIS_PASSWORD = '您的Redis密码'
```

**启动方式**:
```bash
cd cloud_service
pip install -r requirements.txt
python app.py
```

---

### local_service (本地机器)

**文件位置**: `local_service/`

**功能**:
- `monitor_service.py` - 本地监控服务
- `miniqmt_redis.py` - Redis 信号接收与下单（已更新，支持成交记录落库）
- `db_manager.py` - 数据库管理（已更新，新增成交记录表）
- `requirements.txt` - Python 依赖

**monitor_service.py 监控内容**:
1. **Redis 联通性**: 本地 Redis + 云端 Redis
2. **信号接收进程**: `miniqmt_redis.py` 是否在运行
3. **MiniQMT 进程**: xtdata.exe, xttrader.exe 等是否在运行
4. **系统资源**: CPU、内存、磁盘使用率

**配置修改**:
修改 `monitor_service.py` 中的配置：
```python
# 本地 Redis（用于接收信号）
LOCAL_REDIS_HOST = 'localhost'
LOCAL_REDIS_PORT = 6379
LOCAL_REDIS_PASSWORD = None

# 云端 Redis（用于上报数据）
CLOUD_REDIS_HOST = '您的云端Redis地址'
CLOUD_REDIS_PORT = 10973
CLOUD_REDIS_PASSWORD = '您的Redis密码'
```

**启动方式**:
```bash
cd local_service
pip install -r requirements.txt
python monitor_service.py
```

---

## 部署步骤

### 第一步：部署云端 (cloud_service)

1. 将 `cloud_service/` 文件夹上传到云服务器
2. 修改 `app.py` 中的 Redis 配置
3. 安装依赖并启动：
   ```bash
   cd cloud_service
   pip install flask redis
   python app.py
   ```
4. 访问 `http://云服务器IP:5000` 测试

### 第二步：配置本地 (local_service)

1. 确保本地已安装 Redis
2. 修改 `monitor_service.py` 中的配置
3. 修改 `miniqmt_redis.py` 中的 Redis 和 MiniQMT 配置
4. 初始化数据库（如果还没有）：
   ```bash
   cd local_service
   python db_manager.py  # 会创建示例策略表
   ```
5. 启动监控服务：
   ```bash
   python monitor_service.py
   ```
6. 在另一个终端启动下单服务：
   ```bash
   python miniqmt_redis.py
   ```

### 第三步：访问监控面板

在任意浏览器中访问：
```
http://您的云服务器IP:5000
```

## 监控面板功能

### 1. Redis 连接状态
- 显示云端 Redis 连接状态
- 显示策略流数量
- 显示总消息数
- 列出所有策略流及其消息数

### 2. 本地服务状态
- 本地 Redis 连接状态
- 信号接收进程运行状态
- MiniQMT 进程运行状态

### 3. 系统资源
- CPU 使用率
- 内存使用率
- 磁盘使用率

### 4. 最近交易记录
- 显示最近的成交记录
- 包含时间、策略、代码、操作、价格、数量

## 常见问题

### Q: 为什么页面显示一堆字符串而不是渲染的 HTML？
A: 请确保：
1. 访问的是 `http://IP:5000/` 而不是直接打开文件
2. Flask 应用正常运行
3. 先访问 `/simple` 测试页面验证基本功能

### Q: 本地监控数据没有显示？
A: 检查：
1. `monitor_service.py` 是否在运行
2. 云端 Redis 配置是否正确
3. 查看 `monitor_service.py` 的日志输出

### Q: 如何确认数据上报成功？
A: 可以使用 redis-cli 连接云端 Redis 查看：
```bash
redis-cli -h 您的Redis地址 -p 10973 -a 密码
GET monitor:status
```

### Q: 成交记录保存在哪里？
A: 保存在本地 SQLite 数据库 `strategy_data.db` 的 `trade_records` 表中。

## 技术栈

- **Web 框架**: Flask
- **缓存/消息队列**: Redis
- **数据库**: SQLite
- **进程监控**: psutil
- **交易接口**: xtquant (miniQMT)
