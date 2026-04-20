# coding:utf-8
import sqlite3


class DatabaseManager:
    def __init__(self, db_name):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def create_table(self, table_name, columns):
        """
        创建表
        :param table_name: 表名
        :param columns: 列定义，例如 "id INTEGER PRIMARY KEY, name TEXT"
        """
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
        self.cursor.execute(create_table_query)
        self.conn.commit()

    def insert_data(self, table_name, data):
        """
        插入数据
        :param table_name: 表名
        :param data: 数据，以字典形式，例如 {"name": "John", "age": 25}
        """
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?' for _ in data])
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        values = tuple(data.values())
        self.cursor.execute(insert_query, values)
        self.conn.commit()

    def update_data(self, table_name, update_dict, condition):
        """
        更新数据
        :param table_name: 表名
        :param update_dict: 要更新的内容，字典形式，例如 {"age": 26}
        :param condition: 更新条件，例如 "id = 1"
        """
        set_clause = ', '.join([f"{key} =?" for key in update_dict])
        update_query = f"UPDATE {table_name} SET {set_clause} WHERE {condition}"
        values = tuple(update_dict.values())
        self.cursor.execute(update_query, values)
        self.conn.commit()

    def delete_data(self, table_name, condition):
        """
        删除数据
        :param table_name: 表名
        :param condition: 删除条件，例如 "id = 1"
        """
        delete_query = f"DELETE FROM {table_name} WHERE {condition}"
        self.cursor.execute(delete_query)
        self.conn.commit()

    def query_data(self, table_name, columns='*', condition=None):
        """
        查询数据
        :param table_name: 表名
        :param columns: 要查询的列，默认 '*' 代表所有列
        :param condition: 查询条件，可选，例如 "age > 20"
        """
        query = f"SELECT {columns} FROM {table_name}"
        if condition:
            query += f" WHERE {condition}"
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def get_all_strategy_names(self):
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = self.cursor.fetchall()
        table_names = [table[0] for table in tables]
        return table_names

    def create_strategy_table(self, strategy_name, initial_funds):
        table_name = f"{strategy_name}"
        self.cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                code TEXT PRIMARY KEY,
                position INTEGER DEFAULT 0,
                available_funds REAL
            )
        ''')
        self.cursor.execute(f'''
            INSERT INTO {table_name} (code, available_funds) VALUES ('total_funds', ?)
        ''', (initial_funds,))
        # self.cursor.execute(f'''
        #     INSERT INTO {table_name} (code, position) VALUES ('511010.SH', 0)
        # ''')
        # self.cursor.execute(f'''
        #     INSERT INTO {table_name} (code, position) VALUES ('511880.SH', 0)
        # ''')
        self.conn.commit()

    def get_position(self, strategy_name, code):
        table_name = f"{strategy_name}"
        self.cursor.execute(f"SELECT position FROM {table_name} WHERE code=?", (code,))
        row = self.cursor.fetchone()
        return row[0] if row else 0

    def get_available_funds(self, strategy_name):
        table_name = f"{strategy_name}"
        self.cursor.execute(f"SELECT available_funds FROM {table_name} WHERE code='total_funds'")
        row = self.cursor.fetchone()
        return row[0] if row else 0

    def update_position_and_funds(self, strategy_name, code, position_change, funds_change):
        print(f"update_position_and_funds: strategy_name {strategy_name}, code {code}, "
              f"position_change {position_change}, funds_change {funds_change}")
        table_name = f"{strategy_name}"
        # 检查是否存在 code 列为 '600001.sh' 的行
        self.cursor.execute(f"""SELECT * FROM {table_name} WHERE code='{code}'""")
        row_exists = self.cursor.fetchone()
        if not row_exists:
            # 插入新行
            self.cursor.execute(f"""INSERT INTO {table_name} (code, position) VALUES ('{code}', 0)""")
        self.cursor.execute(f'''
            UPDATE {table_name} SET position = position + ? WHERE code=?
        ''', (position_change, code))
        self.cursor.execute(f'''
            UPDATE {table_name} SET available_funds = available_funds + ? WHERE code='total_funds'
        ''', (funds_change,))
        self.conn.commit()

    def create_trade_record_table(self):
        """创建成交记录总表"""
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS trade_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                strategy_name TEXT NOT NULL,
                stock_code TEXT NOT NULL,
                order_type TEXT NOT NULL,
                traded_price REAL NOT NULL,
                traded_volume INTEGER NOT NULL,
                traded_amount REAL NOT NULL,
                order_id TEXT,
                commission REAL,
                UNIQUE(order_id, stock_code)
            )
        ''')
        self.conn.commit()

    def insert_trade_record(self, trade_data):
        """
        插入成交记录
        
        :param trade_data: 字典，包含以下键：
            - timestamp: 时间戳
            - strategy_name: 策略名称
            - stock_code: 股票代码
            - order_type: 订单类型 (BUY/SELL)
            - traded_price: 成交价格
            - traded_volume: 成交数量
            - traded_amount: 成交金额
            - order_id: 订单ID (可选)
            - commission: 手续费 (可选)
        """
        try:
            self.cursor.execute('''
                INSERT OR IGNORE INTO trade_records 
                (timestamp, strategy_name, stock_code, order_type, traded_price, 
                 traded_volume, traded_amount, order_id, commission)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                trade_data.get('timestamp'),
                trade_data.get('strategy_name'),
                trade_data.get('stock_code'),
                trade_data.get('order_type'),
                trade_data.get('traded_price'),
                trade_data.get('traded_volume'),
                trade_data.get('traded_amount'),
                trade_data.get('order_id'),
                trade_data.get('commission', 0)
            ))
            self.conn.commit()
            return True
        except Exception as e:
            print(f"插入成交记录失败: {e}")
            return False

    def query_trade_records(self, strategy_name=None, limit=100):
        """
        查询成交记录
        
        :param strategy_name: 策略名称，可选，不传则查询所有
        :param limit: 返回记录数限制
        :return: 成交记录列表
        """
        if strategy_name:
            self.cursor.execute('''
                SELECT * FROM trade_records 
                WHERE strategy_name = ? 
                ORDER BY timestamp DESC 
                LIMIT ?
            ''', (strategy_name, limit))
        else:
            self.cursor.execute('''
                SELECT * FROM trade_records 
                ORDER BY timestamp DESC 
                LIMIT ?
            ''', (limit,))
        return self.cursor.fetchall()

    def close(self):
        """关闭数据库连接"""
        self.cursor.close()
        self.conn.close()

if __name__ == "__main__":
    # 初始化数据库管理器
    db_manager = DatabaseManager('strategy_data.db')
    # 使用策略名称和初始资金初始化策略数据表
    db_manager.create_strategy_table(strategy_name="g9small", initial_funds=100000)
    # db_manager.create_strategy_table("my_test", 100000)
    print(db_manager.get_all_strategy_names())

    # db_manager.update_position_and_funds("my_test", '511880.SH',500,-10000)
    # db_manager.update_position_and_funds("my_test", '511990.SH',500,-10000)
    db_manager.close()