
# 简单的配置检查脚本
import sys
print("Python 版本:", sys.version)
print()

try:
    import redis
    print("✓ redis 模块已安装")
except ImportError:
    print("✗ redis 模块未安装，请运行: pip install redis")
    sys.exit(1)

print()
print("尝试连接云端 Redis...")
print("主机: 8.153.77.252")
print("端口: 6378")
print("密码: 7712")
print()

try:
    r = redis.StrictRedis(
        host='8.153.77.252',
        port=6378,
        password='7712',
        decode_responses=True,
        socket_timeout=5
    )
    print("正在 ping...")
    result = r.ping()
    print(f"✓ 连接成功! ping: {result}")
    
    print()
    print("查看 monitor:status...")
    data = r.get('monitor:status')
    if data:
        print("✓ monitor:status 存在")
        print("内容前 100 字符:", data[:100])
    else:
        print("- monitor:status 不存在（monitor_service 可能还没运行）")
        
except Exception as e:
    print(f"✗ 连接失败: {type(e).__name__}: {e}")
    print()
    import traceback
    traceback.print_exc()

