# QMT 自动重启功能说明

## 功能描述

当检测到以下情况时，会自动重启 QMT：
- ✅ miniqmt_redis.py 进程正在运行
- ❌ QMT 进程未运行（QMT.exe, xtdata.exe, xttrader.exe 等）

## 配置说明

在 `config.py` 中可以配置：

```python
# MiniQMT 可执行文件路径（用于自动重启）
MINIQMT_EXE = r"D:\国金QMT交易端模拟\userdata_mini\QMT.exe"

# 是否启用自动重启 MiniQMT
AUTO_RESTART_MINIQMT = True
```

## 工作原理

1. **检测频率**：每 30 秒（HEARTBEAT_INTERVAL）检查一次进程状态
2. **触发条件**：miniqmt_redis 运行 && QMT 未运行
3. **冷却机制**：两次重启之间至少间隔 120 秒，避免频繁重启
4. **日志记录**：所有操作都会记录到日志中

## 日志示例

```
2026-04-21 10:00:00 - monitor_service - WARNING - ⚠️ 检测到 miniqmt_redis 运行但 QMT 未运行，尝试自动重启 QMT...
2026-04-21 10:00:00 - monitor_service - INFO - 正在启动 MiniQMT: D:\国金QMT交易端模拟\userdata_mini\QMT.exe
2026-04-21 10:00:00 - monitor_service - INFO - ✅ MiniQMT 启动命令已执行
```

## 手动禁用自动重启

如果不需要自动重启功能，在 `config.py` 中设置：

```python
AUTO_RESTART_MINIQMT = False
```

## 注意事项

1. **路径配置**：确保 `MINIQMT_EXE` 路径正确指向 QMT 可执行文件
2. **权限**：monitor_service 需要有启动 QMT 程序的权限
3. **冷却时间**：120 秒冷却期可以防止 QMT 崩溃时无限重启
4. **进程检测**：会检测多种 QMT 相关进程（QMT.exe, xtdata.exe, xttrader.exe, miniQMT.exe）

## 故障排查

### QMT 没有自动启动
- 检查日志看是否有错误信息
- 确认 `MINIQMT_EXE` 路径是否正确
- 确认 `AUTO_RESTART_MINIQMT` 是否为 True

### QMT 频繁重启
- 检查 QMT 是否真的在正常运行
- 查看日志看是否有冷却提示
- 可能需要增加冷却时间
