import redis
import json
import time

# ===================== 【必须和你聚宽代码一致】 =====================
REDIS_HOST = "8.153.77.252"
REDIS_PORT = 6378
REDIS_PASSWORD = "7712"
STREAM_KEY = "ETF_OverRate"  # 你的策略名，不用改
# ==================================================================

def main():
    print("=" * 60)
    print("        聚宽 Redis Stream 信号接收程序（专用版）")
    print(f"  监听 Stream：{STREAM_KEY}   |   服务器：{REDIS_HOST}:{REDIS_PORT}")
    print("=" * 60)
    print()

    try:
        r = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            decode_responses=True,
            socket_connect_timeout=10
        )

        r.ping()
        print("✅ Redis 连接成功！正在等待交易信号...\n")

        # 从最旧的消息开始读（测试最稳）
        last_id = "0"

        while True:
            messages = r.xread(
                streams={STREAM_KEY: last_id},
                count=1,
                block=500
            )

            if messages:
                for stream, msg_list in messages:
                    for msg_id, data in msg_list:
                        print("=" * 80)
                        print(f"📥 收到交易信号 | ID: {msg_id}")
                        print("=" * 80)
                        
                        for k, v in data.items():
                            print(f"  {k:<15} => {v}")

                        print("\n💚 信号接收成功！")
                        last_id = msg_id

            time.sleep(0.1)

    except redis.ConnectionError:
        print("❌ 连接失败！检查IP/端口/密码/安全组")
    except Exception as e:
        print(f"❌ 异常：{str(e)}")

if __name__ == "__main__":
    main()