import pandas as pd
import pyodbc
from sqlalchemy import create_engine
import pymysql
import sys
import mysql.connector


accdb_path = 'C:/Users/09\Desktop/access/2/202402-IMP.accdb' #access数据库
mysql_debase = 'ethiopia'   #mysql数据库名


# 数据库连接信息
access_conn_str = (
    r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
    r'DBQ='+accdb_path
)
# MySQL数据库连接信息
mysql_conn_str = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': 'root',
    'database': mysql_debase,
}


# 连接Microsoft Access数据库
# access_conn = pyodbc.connect(access_conn_str)

# 连接MySQL数据库
mysql_conn = pymysql.connect(**mysql_conn_str)
# 创建目标数据库（如果不存在）
with mysql_conn.cursor() as cursor:
    cursor.execute("CREATE DATABASE IF NOT EXISTS " + mysql_debase)
# 提交更改
mysql_conn.commit()
# 关闭MySQL数据库连接，重新连接到目标数据库
mysql_conn.close()
mysql_conn = pymysql.connect(**mysql_conn_str)

#########################################   access的操作   ##########################################################
#表名
table_name_arr = ['import','export']
# 循环处理每个表名
for table_item in table_name_arr:
     # 检查表是否存在
    with mysql_conn.cursor() as cursor:
        cursor.execute(f"SHOW TABLES LIKE '{table_item}'")
        if not cursor.fetchone():
            print(f"Table '{table_item}' already exists. Skipping...")
            continue  # 如果表不存在，跳过当前循环
    # 查询表结构信息
    cursor = access_conn.cursor()
    columns_info = cursor.columns(table=table_item).fetchall()

    # 获取字段名和数据类型
    columns = [(column.column_name, column.type_name) for column in columns_info]
    reserved_keywords = ['CONDITION', 'ANOTHER_KEYWORD']  # 列出所有可能是MySQL保留关键字的字段名
    for i, col_item in enumerate(columns):
        if col_item[0] in reserved_keywords:
            columns[i] = (col_item[0] + '_0', col_item[1])
       

    # 查询数据并加载到 DataFrame 中
    sql_query = f'SELECT * FROM {table_item}'
    df = pd.read_sql(sql_query, access_conn)
    #将 DataFrame 转换为数组
    data_array = df.values


#########################################   mysql的操作   ##########################################################
    # # 构建CREATE TABLE语句
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_item} ("
    create_table_sql += "id INT AUTO_INCREMENT PRIMARY KEY,"  # 添加id字段作为主键
    for column_name, data_type in columns:
        if data_type == 'VARCHAR':
            data_type = 'VARCHAR(255)'
        elif data_type == "LONGCHAR":
            data_type = 'LONGTEXT'
        create_table_sql += f"{column_name} {data_type}, "
    create_table_sql = create_table_sql[:-2]  # 移除最后一个逗号和空格
    create_table_sql += ");"
    # 连接MySQL数据库
    mysql_conn = pymysql.connect(**mysql_conn_str)
    
    # # 创建MySQL表
    # with mysql_conn.cursor() as cursor:
    #     cursor.execute(create_table_sql)

    # # 提交更改
    # mysql_conn.commit()
    # mysql_conn = mysql.connector.connect(mysql_conn_str)
    # 创建 MySQL 游标对象
    cursor = mysql_conn.cursor()
    # 获取 DataFrame 的列名
    columns = ', '.join(df.columns)

    # 循环遍历 DataFrame 中的每一行，并插入到 MySQL 数据库中
    for index, row in df.iterrows():
        # 准备插入语句
        values = ', '.join([f"'{value}'" for value in row])  # 注意：这里假设所有数据都是字符串类型，如果有其他类型需要根据情况调整
        insert_query = f"INSERT INTO import ({columns}) VALUES ({values})"
        
        # 执行插入操作
        cursor.execute(insert_query)

    # 提交事务
    mysql_conn.commit()

# 关闭连接
cursor.close()
mysql_conn.close()



    # 将 DataFrame 数据插入到 MySQL 数据库中
    # df.to_sql(table_item, mysql_conn, if_exists='append', index=False)
# 关闭Microsoft Access数据库连接
access_conn.close()
    






#########################################   mysql的操作 end  ##########################################################


# # 将 DataFrame 保存为 CSV 文件
# df.to_csv('output.csv', index=False)

    

# 执行查询
# sql_query = 'SELECT * FROM import'
# cursor.execute(sql_query)

# # # 从游标中获取数据并加载到DataFrame中
# rows = cursor.fetchall()
# columns = [column[0] for column in cursor.description]
# df = pd.DataFrame.from_records(rows, columns=columns)


# print(df)