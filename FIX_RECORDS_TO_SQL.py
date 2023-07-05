from FeishuBitableAPI import FeishuBitableAPI
import configparser
import mysql.connector
from FeishuBitableAPI import LIST_RECORDS


# 创建 FeishuBitableAPI 类的实例
api = FeishuBitableAPI()

def fetch_common_fields(config, feishu_data):
    feishu_fields = set(feishu_data[0]['fields'].keys())
    mydb = mysql.connector.connect(
        host=config.get('DB_BAK', 'host'),
        user=config.get('DB_BAK', 'user'),
        password=config.get('DB_BAK', 'password'),
        database=config.get('DB_BAK', 'database'),
        port=config.get('DB_BAK', 'port'),
    )
    mycursor = mydb.cursor()
    mycursor.execute(f"SHOW COLUMNS FROM {config.get('DB_BAK', 'table')}")
    db_fields = set([field[0] for field in mycursor.fetchall()])
    common_fields = feishu_fields.intersection(db_fields)

    return common_fields, mydb, mycursor


def check_and_update(config, common_fields, feishu_data, mydb, mycursor):
    key = config.get('DB_BAK', 'KEY')
    for record in feishu_data:
        sql = f"SELECT * FROM {config.get('DB_BAK', 'table')} WHERE {key} = %s"
        val = (record['fields'][key], )
        mycursor.execute(sql, val)
        result = mycursor.fetchall()

        if result:  # if key exists in database
            for field in common_fields:
                if record['fields'].get(field) != result[0][field]:
                    update_sql = f"UPDATE {config.get('DB_BAK', 'table')} SET {field} = %s WHERE {key} = %s"
                    update_val = (record['fields'].get(field), record['fields'][key])
                    mycursor.execute(update_sql, update_val)
        else:  # if key does not exist in database
            insert_sql = f"INSERT INTO {config.get('DB_BAK', 'table')} ({', '.join(common_fields)}) VALUES ({', '.join(['%s']*len(common_fields))})"
            insert_val = tuple(record['fields'].get(field) for field in common_fields)
            mycursor.execute(insert_sql, insert_val)

        mydb.commit()


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
    if view_id is None:
        view_id = config.get('ID', 'view_id')
    if not page_token:
        page_token = config.get('ADD_RECORDS', 'page_token', fallback=None)
    if not page_size:
        page_size = config.get('ADD_RECORDS', 'page_size', fallback=500)

    feishu_data = []
    response = LIST_RECORDS(app_token=app_token, table_id=table_id, page_token=page_token, page_size=page_size, config_file=config_file)
    feishu_data.extend(response['data']['items'])
    while response['data']['has_more']:
        response = LIST_RECORDS(page_token=response['data']['page_token'], config_file=config_file)
        feishu_data.extend(response['data']['items'])

    common_fields, mydb, mycursor = fetch_common_fields(config, feishu_data)
    check_and_update(config, common_fields, feishu_data, mydb, mycursor)


if __name__ == "__main__":
    FIX_RECORDS_TO_SQL()