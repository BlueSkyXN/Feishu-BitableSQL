import requests
import configparser
import json
import pandas as pd
import pymysql
from FeishuBitableAPI import FeishuBitableAPI

# 创建 FeishuBitableAPI 类的实例
api = FeishuBitableAPI()

def UPDATE_RECORDS_FROM_SQL(app_token=None, table_id=None, view_id=None, page_token=None, page_size=None, config_file=None, field_file=None):
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
    if view_id is None:
        view_id = config.get('ID', 'view_id')
    if not page_token:
        page_token = config.get('ADD_RECORDS', 'page_token', fallback=None)
    if not page_size:
        page_size = config.get('ADD_RECORDS', 'page_size', fallback=100)

    # 从配置文件中读取数据库信息和SQL查询
    db_info = {
        'host': config.get('DB', 'host'),
        'user': config.get('DB', 'user'),
        'password': config.get('DB', 'password'),
        'database': config.get('DB', 'database'),
        'port': config.getint('DB', 'port')
    }
    sql_query = config.get('SQL', 'sql_query')

    # 连接到数据库并执行SQL查询
    conn = pymysql.connect(**db_info)
    df = pd.read_sql_query(sql_query, conn)
    conn.close()

    # 将DataFrame转换为字典，以便可以将其作为JSON发送
    records = df.astype(str).to_dict('records')

    # 设置请求头
    headers = {
        "Authorization": f"Bearer {user_access_token}",
        "Content-Type": "application/json; charset=utf-8"
    }

    print(f"Total records to be updated: {len(records)}")

    # 初始化 response 变量为 None
    response = None

    # 检查记录数量，如果超过450则开始分片处理
    batch_size = 450  # 每次发送的记录数量
    batch_records = []  # 空的 batch_records 列表

    # 获取飞书表格中的记录
    feishu_records = LIST_RECORDS(app_token=app_token, table_id=table_id, page_token=page_token, page_size=page_size, config_file=config_file)

    for i in range(0, len(records), batch_size):
        current_batch_records = records[i:i+batch_size]  # 获取当前批次的记录
        batch_start = i + 1
        batch_end = min(i + batch_size, len(records))
        print(f"Processing records {batch_start} to {batch_end}...")

        # 对于每个批次，都应该重构请求体
        batch_request_body = {'records': []}

        for record in current_batch_records:
            # 查找飞书表格中的对应记录
            feishu_record = next((item for item in feishu_records['data']['items'] if item['fields']['KEY'] == record['KEY']), None)

            # 如果飞书表格中没有这条记录，或者记录的内容有差异，就将这条记录添加到更新列表中
            if feishu_record is None or feishu_record['fields'] != record:
                batch_request_body['records'].append({'fields': record, 'record_id': feishu_record['record_id'] if feishu_record else None})

        batch_records.extend(batch_request_body['records'])  # 将当前批次的记录添加到 batch_records

        # 如果没有需要更新的记录，就跳过这个批次
        if not batch_request_body['records']:
            continue

        # 构建请求URL
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_update"
        print(f"URL set to: {url}")

        print(f"Request body: {json.dumps(batch_request_body, indent=2)}")

        # 发送请求并接收响应
        response = requests.post(url, headers=headers, json=batch_request_body)
        print("Request sent. Response received.")
        print(response.text)
        print("Request ok.")

        # 检查响应状态
        if response.status_code == 200:
            response_json = response.json()
            if response_json.get('code') == 0:
                print(f"Successfully updated table records. Response status code: {response.status_code}, Response code: {response_json.get('code')}")
            else:
                print(f"Error in updating table records. Response status code: {response.status_code}, Response code: {response_json.get('code')}")
                response.raise_for_status()
        else:
            print(f"Error in updating table records. Response status code: {response.status_code}")
            response.raise_for_status()

    ENABLE_UPDATE_RECORDS = True
    
    if ENABLE_UPDATE_RECORDS:
        if field_file is None:
           field_file = 'feishu-field.ini'
        # 更新field配置文件
        field_config = configparser.ConfigParser()
        field_config.read('feishu-field.ini', encoding='utf-8')
        if "UPDATE_RECORDS_FROM_SQL" not in field_config.sections():
            field_config.add_section("UPDATE_RECORDS_FROM_SQL")
        field_config.set("UPDATE_RECORDS_FROM_SQL", "request_body", json.dumps({"records": batch_records}))
        field_config.set("UPDATE_RECORDS_FROM_SQL", "response_body", response.text)
        with open('feishu-field.ini', 'w', encoding='utf-8') as field_configfile:
            field_config.write(field_configfile)
            print("Request body and response body saved to feishu-field.ini.")

if __name__ == "__main__":
    UPDATE_RECORDS_FROM_SQL()
