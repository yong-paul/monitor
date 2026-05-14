FROM python:3.10-slim

WORKDIR /app

# 一次性拷贝所有文件（除了被 .dockerignore 排除的）
COPY . .

RUN pip install -i https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt

CMD ["python", "app.py"]