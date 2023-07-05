from FeishuBitableAPI import FeishuBitableAPI
import configparser
import mysql.connector

# 创建 FeishuBitableAPI 类的实例
api = FeishuBitableAPI()

def fetch_common_fields(config, feishu_data):
    if config is None:
        config = 'feishu-config.ini'
    feishu_fields = set(feishu_data[0]['fields'].keys())
    print("Feishu Fields:", feishu_fields)

    mydb = mysql.connector.connect(
        host=config.get('DB_BAK', 'host'),
        user=config.get('DB_BAK', 'user'),
        password=config.get('DB_BAK', 'password'),
        database=config.get('DB_BAK', 'database'),
        port=config.get('DB_BAK', 'port'),
    )
    print("Connected to MySQL database")

    mycursor = mydb.cursor()
    mycursor.execute(f"SHOW COLUMNS FROM {config.get('DB_BAK', 'table')}")
    db_fields = set([field[0] for field in mycursor.fetchall()])
    print("Database Fields:", db_fields)

    common_fields = feishu_fields.intersection(db_fields)
    print("Common Fields:", common_fields)

    return common_fields, mydb, mycursor

def check_and_update(config, common_fields, feishu_data, mydb, mycursor, field_file=None):
    if config is None:
        config = 'feishu-config.ini'
    if field_file is None:
        field_file = 'feishu-field.ini'
    key = config.get('DB_BAK', 'KEY')
    
    mycursor.execute(f"SHOW COLUMNS FROM {config.get('DB_BAK', 'table')}")
    db_fields = [field[0] for field in mycursor.fetchall()]
    #print("Database Fields:", db_fields)

    common_fields = set(common_fields).intersection(db_fields)
    #print("Common Fields:", common_fields)

    # 获取查询结果的字段列表
    columns = [desc[0] for desc in mycursor.description]

    for record in feishu_data:
        sql = f"SELECT * FROM {config.get('DB_BAK', 'table')} WHERE {key} = %s"
        val = (record['fields'][key], )
        mycursor.execute(sql, val)
        result = mycursor.fetchall()

        #print("Record:", record)  # 打印当前处理的记录
        #print("Result:", result)  # 打印数据库查询结果

        if result:  # if key exists in database
            for field in common_fields:
                #print("Field:", field)  # 打印当前处理的字段
                #print("Feishu Value:", record['fields'].get(field))  # 打印飞书字段值
                
                db_values = dict(zip(columns, result[0]))  # 转换为字典类型
                #print("DB Value:", db_values.get(field))  # 打印数据库字段值

                if record['fields'].get(field) != db_values.get(field):
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

    print("Config File:", config_file)
    print("Field File:", field_file)

    # 读取配置文件
    config = configparser.ConfigParser()
    config.read(config_file, encoding='utf-8')

    # 提取tokens和app_token
    user_access_token = config.get('TOKEN', 'user_access_token')
    print("User Access Token:", user_access_token)

    # 仅在未提供输入参数时从配置文件中读取
    if app_token is None:
        app_token = config.get('TOKEN', 'app_token')
        print("App Token (from config file):", app_token)
    else:
        print("App Token (from input):", app_token)

    if table_id is None:
        table_id = config.get('ID', 'table_id')
        print("Table ID (from config file):", table_id)
    else:
        print("Table ID (from input):", table_id)

    if not page_token:
        page_token = config.get('ADD_RECORDS', 'page_token', fallback=None)
        print("Page Token (from config file):", page_token)
    else:
        print("Page Token (from input):", page_token)

    if not page_size:
        page_size = config.get('ADD_RECORDS', 'page_size', fallback=500)
        print("Page Size (from config file):", page_size)
    else:
        print("Page Size (from input):", page_size)

    feishu_data = []
    response = api.LIST_RECORDS(app_token=app_token, table_id=table_id, page_token=page_token, page_size=page_size, config_file=config_file)
    feishu_data.extend(response['data']['items'])
    print("Fetched records:", len(feishu_data))

    while response['data']['has_more']:
        response = api.LIST_RECORDS(page_token=response['data']['page_token'], config_file=config_file)
        feishu_data.extend(response['data']['items'])
        print("Fetched more records. Total records:", len(feishu_data))

    common_fields, mydb, mycursor = fetch_common_fields(config, feishu_data)
    print("Common Fields:", common_fields)

    check_and_update(config, common_fields, feishu_data, mydb, mycursor, field_file=field_file)  
    # 更新函数调用，传递field_file参数



if __name__ == "__main__":
    FIX_RECORDS_TO_SQL()
