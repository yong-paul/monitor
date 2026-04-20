
# coding:utf-8
import socket
import sys

print('开始测试云端 Redis 连接...')
print('主机: 8.153.77.252, 端口: 6378')

try:
    # 测试 socket 连接
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    result = sock.connect_ex(('8.153.77.252', 6378))
    if result == 0:
        print('✓ 网络端口可访问')
        sock.close()
    else:
        print(f'✗ 网络端口无法访问，错误码: {result}')
        print('可能原因:')
        print('  1. 云服务器防火墙未开放 6378 端口')
        print('  2. Redis 服务未启动')
        print('  3. Redis 仅监听本地地址 (127.0.0.1)')
except Exception as e:
    print(f'✗ 连接异常: {e}')

print('\n测试完成。')

