import pymysql
import requests
import configparser
import json
from FeishuBitableAPI import FeishuBitableAPI

# 创建 FeishuBitableAPI 类的实例
api = FeishuBitableAPI()

def upload_records_from_sql(app_token=None, table_id=None, view_id=None, page_token=None, page_size=None, config_file=None):
    if config_file is None:
        config_file = 'feishu-config.ini'

    # 读取配置文件
    config = configparser.ConfigParser()
    config.read(config_file, encoding='utf-8')

    # 从配置文件获取tokens和app_token
    user_access_token = config.get('TOKEN', 'user_access_token')

    # 从配置文件获取SQL查询语句
    sql_query = config.get('SQL', 'sql_query')

    # 建立数据库连接
    db_host = config.get('DB', 'host')
    db_user = config.get('DB', 'user')
    db_password = config.get('DB', 'password')
    db_database = config.get('DB', 'database')

    conn = pymysql.connect(host=db_host, user=db_user, password=db_password, database=db_database)
    cursor = conn.cursor()

    # 执行SQL查询
    cursor.execute(sql_query)
    result = cursor.fetchall()

    # 关闭数据库连接
    cursor.close()
    conn.close()

    # 构建记录列表
    records = []
    for row in result:
        record = {}
        for i, value in enumerate(row):
            field_name = cursor.description[i][0]
            record[field_name] = value
        records.append(record)

    # 设置请求头
    headers = {
        "Authorization": f"Bearer {user_access_token}",
        "Content-Type": "application/json; charset=utf-8"
    }

    # 检查记录数量，如果超过450则开始分片处理
    batch_size = 450  # 每次发送的记录数量
    for i in range(0, len(records), batch_size):
        batch_records = records[i:i+batch_size]  # 获取当前批次的记录
        # 对于每个批次，都应该重构请求体
        batch_request_body = {'records': batch_records}

        # 构建请求URL
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create"
        print(f"URL set to: {url}")

        # 发送请求并接收响应
        response = requests.post(url, headers=headers, json=batch_request_body)
        print("Request sent. Response received.")

        # 检查响应状态
        if response.status_code == 200:
            response_json = response.json()
            if response_json.get('code') == 0:
                print(f"Successfully created table records. Response status code: {response.status_code}, Response code: {response_json.get('code')}")
            else:
                print(f"Error in creating table records. Response status code: {response.status_code}, Response code: {response_json.get('code')}")
                # 如果响应中包含 "FieldNameNotFound" 错误，尝试修复并重试
                if response_json.get("code") == 1254045:
                    print("检测到FieldNameNotFound错误，尝试创建不存在的字段...")

                    api.CHECK_FIELD_EXIST(app_token=app_token, table_id=table_id, view_id=view_id, page_token=page_token, page_size=page_size, csv_file=csv_file, config_file=config_file)

                    print("重试添加记录...")
                    response = requests.post(url, headers=headers, json=batch_request_body)
                    response_json = response.json()

                    if response.status_code != 200 or response_json.get('code') != 0:
                        print(f"重试失败，无法添加记录。错误信息: {response.json()}")
                        response.raise_for_status()
                else:
                    response.raise_for_status()
        else:
            print(f"Error in creating table records. Response status code: {response.status_code}")
            response.raise_for_status()

    ENABLE_ADD_RECORDS = False
    
    if ENABLE_ADD_RECORDS:
        if field_file is None:
           field_file = 'feishu-field.ini'
        # 更新field配置文件
        field_config = configparser.ConfigParser()
        field_config.read('feishu-field.ini', encoding='utf-8')
        if "ADD_RECORDS_FROM_CSV" not in field_config.sections():
            field_config.add_section("ADD_RECORDS_FROM_CSV")
        field_config.set("ADD_RECORDS_FROM_CSV", "request_body", json.dumps({"records": batch_records}))
        field_config.set("ADD_RECORDS_FROM_CSV", "response_body", response.text)
        with open('feishu-field.ini', 'w', encoding='utf-8') as field_configfile:
            field_config.write(field_configfile)
            print("Request body and response body saved to feishu-field.ini.")

if __name__ == "__main__":
    upload_records_from_sql()
