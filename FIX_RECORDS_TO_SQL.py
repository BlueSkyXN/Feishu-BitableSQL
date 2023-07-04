import requests
import configparser
import json
import pandas as pd
import pymysql
from FeishuBitableAPI import FeishuBitableAPI

# 创建 FeishuBitableAPI 类的实例
api = FeishuBitableAPI()

def FIX_RECORDS_TO_SQL(app_token=None, table_id=None, key_field=None, page_token=None, page_size=None, config_file=None, field_file=None):
    if config_file is None:
        config_file = 'feishu-config.ini'
    if field_file is None:
        field_file = 'feishu-field.ini'

    # 读取配置文件
    config = configparser.ConfigParser()
    config.read(config_file, encoding='utf-8')

    # 提取tokens和app_token
    user_access_token = config.get('TOKEN', 'user_access_token')

    # 仅在未提供输入参数时从配置文件中读取
    if app_token is None:
        app_token = config.get('TOKEN', 'app_token')
    if table_id is None:
        table_id = config.get('ID', 'table_id')
    if not page_token:
        page_token = config.get('UPDATE_RECORDS', 'page_token', fallback=None)
    if not page_size:
        page_size = config.get('UPDATE_RECORDS', 'page_size', fallback=100)
    if not key_field:
        key_field = config.get('UPDATE_RECORDS', 'KEY', fallback='ID')

    # 从配置文件中读取数据库信息和SQL查询
    db_info = {
        'host': config.get('DB', 'host'),
        'user': config.get('DB', 'user'),
        'password': config.get('DB', 'password'),
        'database': config.get('DB', 'database'),
        'port': config.getint('DB', 'port')
    }

    # 连接到数据库
    conn = pymysql.connect(**db_info)
    cursor = conn.cursor()

    # 获取飞书表格中的所有记录
    feishu_records_set = set()
    page_token = None
    while True:
        feishu_records = api.LIST_RECORDS(app_token=app_token, table_id=table_id, page_token=page_token, page_size=page_size, config_file=config_file)
        page_token = feishu_records.get('data', {}).get('page_token')
        feishu_records_set.update(item['fields'].get(key_field) for item in feishu_records['data']['items'])
        if not feishu_records.get('data', {}).get('has_more'):
            break

    # 检查飞书中的记录是否在数据库中存在，如果不存在，就将这些记录添加到数据库中
    for record in feishu_records_set:
        sql_query = f"SELECT * FROM wp_posts WHERE {key_field} = '{record}'"
        cursor.execute(sql_query)
        result = cursor.fetchone()
        if result is None:
            # 如果数据库中没有这条记录，就添加整条记录
            # 注意：这里假设你的表格有一个名为 'field1' 的字段，你需要根据你的实际情况修改这个字段名
            sql_insert = f"INSERT INTO wp_posts ({key_field}) VALUES ('{record}')"
            cursor.execute(sql_insert)
            conn.commit()

    # 关闭数据库连接
    cursor.close()
    conn.close()

if __name__ == "__main__":
    FIX_RECORDS_TO_SQL()
