# Cloud Service Docker 部署指南

## 前置条件

1. 已安装 Docker 和 Docker Compose
2. 云端 Redis 服务已配置好（config.py 中的 REDIS_HOST, REDIS_PORT, REDIS_PASSWORD）

## 快速开始

### 方式一：使用 Docker Compose（推荐）

```bash
# 进入 cloud_service 目录
cd cloud_service

# 构建并启动容器（后台运行）
docker-compose up -d --build

# 查看日志
docker-compose logs -f

# 停止容器
docker-compose down

# 重启容器
docker-compose restart
```

### 方式二：直接使用 Docker

```bash
# 构建镜像
docker build -t quant-cloud-service .

# 运行容器（自动重启）
docker run -d \
  --name quant-cloud-service \
  -p 5000:5000 \
  --restart unless-stopped \
  quant-cloud-service

# 查看日志
docker logs -f quant-cloud-service

# 停止容器
docker stop quant-cloud-service

# 删除容器
docker rm quant-cloud-service
```

## 配置说明

### 自动重启策略

在 `docker-compose.yml` 中配置了：
- `restart: unless-stopped` - 容器会自动重启，除非你手动停止

其他可用的重启策略：
- `no` - 不自动重启
- `always` - 总是自动重启
- `on-failure` - 只有失败时才重启
- `unless-stopped` - 自动重启（除非手动停止）- **推荐**

### 端口映射

默认映射 `5000:5000`，如需修改端口，编辑 `docker-compose.yml`：
```yaml
ports:
  - "8080:5000"  # 主机8080端口映射到容器5000端口
```

### 时区设置

已设置为 Asia/Shanghai，如需修改编辑 `docker-compose.yml`：
```yaml
environment:
  - TZ=Asia/Shanghai
```

## 常用命令

```bash
# 查看容器状态
docker-compose ps

# 查看容器日志
docker-compose logs -f cloud-service

# 进入容器
docker-compose exec cloud-service bash

# 更新代码后重新构建
docker-compose up -d --build
```

## 验证服务

启动后访问：`http://localhost:5000` 查看是否正常显示监控页面。

## 注意事项

1. 确保 config.py 中的 Redis 配置正确
2. 如果云端 Redis 需要特殊网络访问，可能需要配置网络
3. 生产环境建议使用 Gunicorn 或 uWSGI 代替 Flask 开发服务器
