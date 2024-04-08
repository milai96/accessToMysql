import pandas as pd
from sqlalchemy import create_engine
import pyodbc
import os
import csv
import configparser

# 创建一个配置对象
config = configparser.ConfigParser()
config.read('config.ini')

def access_to_csv(accdb_path):
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
    elif 'export' in file_name:
        table_name = 'export'
    elif 'import' in file_name:
        table_name = 'import'
    else:
        name, ext = os.path.splitext(file_name)
        table_name = name.split('.')[0]

    # 数据库连接信息
    access_conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        r'DBQ=' + accdb_path
    )


    conn = pyodbc.connect(access_conn_str)
    cursor = conn.cursor()

    # 执行 SQL 查询
    query = 'SELECT * FROM '+table_name  # 替换为你的表名
    cursor.execute(query)


    # 处理列名
    column_names = [column[0] for column in cursor.description]
    new_column_names = []
    keywords = config['dev']['keyword']
    for name in column_names:
        for keyword in keywords:
            if keyword == name:
                name += '_0'
                break
        new_column_names.append(name)

    # 将查询结果保存为 CSV 文件
    with open(accdb_path+'.csv', 'w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(new_column_names)  # 写入处理后的列名
        csv_writer.writerows(cursor.fetchall())  # 写入数据
    # 关闭连接
    cursor.close()
    conn.close()
