# 基础镜像
FROM python:3.9-slim

# 设置工作目录
WORKDIR /app

# 设置环境变量（避免 Python 缓冲输出）
ENV PYTHONUNBUFFERED=1 \
    FLASK_APP=app.py \
    TZ=Asia/Shanghai

# 安装系统依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# 复制依赖文件并安装
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

# 复制应用代码
COPY . .

# 暴露端口（与 app.py 中 Flask 运行端口一致）
EXPOSE 5000

# 启动命令（使用 gunicorn 替代内置服务器，更稳定）
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--workers", "2", "app:app"]