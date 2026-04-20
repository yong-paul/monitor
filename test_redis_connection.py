
# coding:utf-8
import redis
import socket

print('=' * 60)
print('云端 Redis 连接测试')
print('=' * 60)

# 云端 Redis 配置
CLOUD_REDIS_HOST = '8.153.77.252'
CLOUD_REDIS_PORT = 6378
CLOUD_REDIS_PASSWORD = '7712'

print(f'\n配置信息:')
print(f'  主机: {CLOUD_REDIS_HOST}')
print(f'  端口: {CLOUD_REDIS_PORT}')
print(f'  密码: {CLOUD_REDIS_PASSWORD}')

# 1. 测试网络连通性
print(f'\n[1/4] 测试网络连通性...')
try:
    sock = socket.create_connection((CLOUD_REDIS_HOST, CLOUD_REDIS_PORT), timeout=5)
    sock.close()
    print('  ✓ 网络连接正常')
except Exception as e:
    print(f'  ✗ 网络连接失败: {e}')
    print('  提示: 请检查云服务器防火墙是否开放 6378 端口')

# 2. 测试 Redis 连接
print(f'\n[2/4] 测试 Redis 连接...')
try:
    r = redis.StrictRedis(
        host=CLOUD_REDIS_HOST,
        port=CLOUD_REDIS_PORT,
        password=CLOUD_REDIS_PASSWORD,
        decode_responses=True,
        socket_timeout=5
    )
    result = r.ping()
    print(f'  ✓ Redis 连接成功 (ping: {result})')
    
    # 3. 检查 monitor:status 键
    print(f'\n[3/4] 检查 monitor:status 键...')
    monitor_data = r.get('monitor:status')
    if monitor_data:
        print(f'  ✓ monitor:status 键存在')
        print(f'  内容长度: {len(monitor_data)} 字节')
    else:
        print(f'  ✗ monitor:status 键不存在 (说明 monitor_service 还没有上报数据)')
    
    # 4. 检查 monitor:history Stream
    print(f'\n[4/4] 检查 monitor:history Stream...')
    try:
        stream_len = r.xlen('monitor:history')
        print(f'  ✓ monitor:history Stream 存在，共 {stream_len} 条记录')
    except:
        print(f'  ✗ monitor:history Stream 不存在或为空')
    
except Exception as e:
    print(f'  ✗ Redis 连接失败: {e}')
    print('\n可能的原因:')
    print('  1. 云服务器防火墙未开放 6378 端口')
    print('  2. Redis 服务未启动')
    print('  3. Redis 配置为仅监听本地 (bind 127.0.0.1)')
    print('  4. 密码错误')

print('\n' + '=' * 60)

