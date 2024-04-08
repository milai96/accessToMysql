import pandas as pd
from sqlalchemy import create_engine
import pyodbc
import pymysql
import sys
import os
import configparser

# 创建一个配置对象
config = configparser.ConfigParser()
config.read('config.ini')

def create_table(accdb_path):
    parent_dir = os.path.dirname(accdb_path)
    last_dir_name = os.path.basename(parent_dir)
    database = last_dir_name  # 数据库名
    
    accdb_path = accdb_path # Access数据库路径

    # 获取文件名
    file_name = accdb_path.split('/')[-1]
    if 'IMP' in file_name:
        table_name = 'import'
    elif 'EXP' in file_name:
        table_name = 'export'
    else:
        name, ext = os.path.splitext(file_name)
        table_name = name.split('.')[0]

    # 数据库连接信息
    access_conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        r'DBQ=' + accdb_path
    )


    # MySQL数据库连接信息
    mysql_conn_str = {
        'host': config['mysql']['host'],
        'user': config['mysql']['username'],
        'password': config['mysql']['password'],
        'database': database,
    }


    # 连接Microsoft Access数据库
    access_conn = pyodbc.connect(access_conn_str)


    # 查询表结构信息
    cursor = access_conn.cursor()
    columns_info = cursor.columns(table=table_name).fetchall()

    # 获取字段名和数据类型
    columns = [(column.column_name, column.type_name) for column in columns_info]
    reserved_keywords = config['dev']['keyword']  # 列出所有可能是MySQL保留关键字的字段名
    for i, col_item in enumerate(columns):
        if col_item[0] in reserved_keywords:
            columns[i] = (col_item[0] + '_0', col_item[1])
    # # 构建CREATE TABLE语句
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ("
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
    # 创建 Cursor 对象
    cursor = mysql_conn.cursor()
    cursor.execute(create_table_sql)
