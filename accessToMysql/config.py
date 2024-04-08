import configparser

# 创建一个配置对象
config = configparser.ConfigParser()

# 写入配置到配置文件
config['mysql'] = {
    'username': 'root',
    'password': 'root',
    'host': '127.0.0.1',
    'port':'3306'
    }

#关键词
config['dev'] = {"keyword":['ANOTHER_KEYWORD', 'CONDITION','ORDER']}

# 将配置写入文件
with open('config.ini', 'w') as configfile:
    config.write(configfile)


