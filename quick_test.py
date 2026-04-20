
import socket
import redis

print("="*60)
print("云端 Redis 连接测试")
print("="*60)
print("主机: 8.153.77.252, 端口: 6378")
print()

# 测试 1: Socket 连接
print("[1/2] 测试网络连接...")
try:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    result = sock.connect_ex(('8.153.77.252', 6378))
    if result == 0:
        print("  ✓ 网络端口可访问")
        sock.close()
    else:
        print(f"  ✗ 网络端口无法访问，错误码: {result}")
        print()
        print("这是最常见的问题！请检查：")
        print("  1. 云服务器安全组/防火墙是否开放了 6378 端口")
        print("  2. Redis 服务是否正在运行")
        print("  3. Redis 是否配置为监听 0.0.0.0 而不是 127.0.0.1")
except Exception as e:
    print(f"  ✗ 连接异常: {e}")

print()

# 测试 2: Redis 连接
print("[2/2] 测试 Redis 连接...")
try:
    r = redis.StrictRedis(
        host='8.153.77.252',
        port=6378,
        password='7712',
        decode_responses=True,
        socket_timeout=5
    )
    ping_result = r.ping()
    print(f"  ✓ Redis 连接成功 (ping: {ping_result})")
    
    monitor_data = r.get('monitor:status')
    if monitor_data:
        print("  ✓ monitor:status 键存在 - 说明 monitor_service 已成功上报数据")
    else:
        print("  - monitor:status 键不存在 - 请先启动 monitor_service.py")
        
except Exception as e:
    print(f"  ✗ Redis 连接失败: {e}")
    print()
    print("可能原因:")
    print("  1. 密码错误")
    print("  2. Redis 配置了 protected-mode")
    print("  3. 网络问题（见测试 1）")

print()
print("="*60)
print("测试完成！请查看 故障排除指南.md 获取详细解决方案")
print("="*60)

