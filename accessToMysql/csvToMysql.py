import pandas as pd
import os
from sqlalchemy import create_engine
import configparser
import sys

# 创建一个配置对象
config = configparser.ConfigParser()
config.read('config.ini')


def csv_to_mysql(accdb_path):
    # CSV文件路径
    csv_file_path = accdb_path+'.csv'

    data_base  = csv_file_path.split('/')[-2]  # 数据库名

    # 获取文件名
    file_name = csv_file_path.split('/')[-1]
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

 
    mysql_conn_str = 'mysql+pymysql://'+config['mysql']['username']+':'+config['mysql']['password']+'@'+config['mysql']['host']+':'+config['mysql']['port']+'/'+data_base
    # 读取CSV文件
    df = pd.read_csv(csv_file_path)
    
    # 处理字段名
    keywords_to_rename = config['dev']['keyword']
    for keyword in keywords_to_rename:
        df.columns = [col + '_0' if col == keyword else col for col in df.columns]

    # 创建到MySQL数据库的引擎
    engine = create_engine(mysql_conn_str)

    # 将DataFrame写入MySQL数据库的表中
    # 如果表已经存在，数据将被追加到表中；如果表不存在，将自动创建表
    df.to_sql(table_name, con=engine, if_exists='append', index=False)
    # 删除文件
    os.remove(csv_file_path)



