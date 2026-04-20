
# 云端 Redis 连接测试脚本 (PowerShell)

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  云端 Redis 连接测试" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

$redis_host = "8.153.77.252"
$port = 6378
$password = "7712"

Write-Host "`n配置信息:" -ForegroundColor Yellow
Write-Host "  主机: $redis_host"
Write-Host "  端口: $port"
Write-Host "  密码: $password"

# 1. 测试网络连接
Write-Host "`n[1/2] 测试网络连接..." -ForegroundColor Yellow
try {
    $testResult = Test-NetConnection -ComputerName $redis_host -Port $port -WarningAction SilentlyContinue
    if ($testResult.TcpTestSucceeded) {
        Write-Host "  ✓ 网络端口可访问" -ForegroundColor Green
    } else {
        Write-Host "  ✗ 网络端口无法访问" -ForegroundColor Red
        Write-Host "`n可能原因:" -ForegroundColor Red
        Write-Host "  1. 云服务器防火墙未开放 $port 端口"
        Write-Host "  2. Redis 服务未启动"
        Write-Host "  3. Redis 仅监听本地地址 (127.0.0.1)"
    }
} catch {
    Write-Host "  ✗ 连接测试失败: $_" -ForegroundColor Red
}

# 2. 尝试用 Python 测试 Redis
Write-Host "`n[2/2] 尝试用 Python 测试 Redis 连接..." -ForegroundColor Yellow
$pythonScript = @"
import redis
try:
    r = redis.StrictRedis(
        host='$redis_host',
        port=$port,
        password='$password',
        decode_responses=True,
        socket_timeout=5
    )
    result = r.ping()
    print('  ✓ Redis 连接成功 (ping: ' + str(result) + ')')
    
    monitor_data = r.get('monitor:status')
    if monitor_data:
        print('  ✓ monitor:status 键存在')
    else:
        print('  - monitor:status 键不存在 (monitor_service 可能还没运行)')
        
except Exception as e:
    print('  ✗ Redis 连接失败: ' + str(e))
"@

try {
    $pythonScript | python
} catch {
    Write-Host "  ✗ Python 测试失败" -ForegroundColor Red
}

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "  测试完成" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "`n请查看 故障排除指南.md 获取详细解决方案" -ForegroundColor Gray

