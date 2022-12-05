'''Websocket-bridge plugin for Database'''
import asyncio
import datetime
import json
import os

import pymysql
import websocket_bridge_python
from sexpdata import dumps

database_configs = {}
connections = {}

class DatabaseConnectInfo(dict):
    '''Database Connect Info Class'''
    def __init__(self, db_type:str, host:str, port:int, user:str, password:str):
        self.db_type = db_type
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.con = None
        dict.__init__(self, db_type=db_type,host=host, port=port, user=user, password=password)
    def get_driver(self):
        '''get database driver by db_type'''
        if self.db_type == 'MySQL':
            import pymysql
            return pymysql
        return None
    def connect(self):
        '''connect database'''
        self.con = self.get_driver().connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            )
        return self.con

async def on_message(message:str):
    '''dispatch message recived from Emacs.'''
    info = json.loads(message)
    cmd = info[1][0].strip()
    if cmd == "run_sql":
        db_name = info[1][1]
        database = info[1][2]
        sql = info[1][3]
        columns, data = await run_sql(db_name, sql, database)
        cmd = f"(websocket-bridge-database-show '{dumps(columns)} '{dumps(format_db_data(data))})"
        await run_and_log(cmd)
    if cmd == "new_database":
        db_name = info[1][1]
        db_type = info[1][2]
        host = info[1][3]
        port = info[1][4]
        user = info[1][5]
        password = info[1][6]
        info = DatabaseConnectInfo(db_type=db_type, host=host, port=port,user=user, password=password)
        await new_database(db_name, info)
    if cmd == "get_db_meta":
        db_name = info[1][1]
        print(database_configs)
        print(dumps(database_configs.get(db_name)))
        print(database_configs.get(db_name))
    if cmd == "show_databases":
        db_name = info[1][1]
        _, data = await run_sql(db_name, "show databases", None)
        if data:
            data = [item[0] for item in data]
            await run_and_log(f"(setq websocket-bridge-database-db-databases '{dumps(data)})")
    if cmd == "show_tables":
        db_name = info[1][1]
        database = info[1][2]
        _, data = await run_sql(db_name, "show tables", database)
        if data:
            data = [item[0] for item in data]
            await run_and_log(f"(setq websocket-bridge-database-db-tables'{dumps(data)})")

bridge = websocket_bridge_python.bridge_app_regist(on_message)

def format_db_data(data):
    "format database data"
    result = []
    if not data:
        return result
    for column_data in data:
        column = []
        for item in column_data:
            if isinstance(item, datetime.datetime):
                column.append(item.isoformat())
            else:
                column.append(item)
        result.append(column)
    return result

async def run_sql(db_name:str, sql:str, database):
    '''run sql in db'''
    try:
        with database_configs.get(db_name).connect().cursor() as cursor:
            if database:
                cursor.execute(f'use {database}')
            cursor.execute(sql)
            if cursor.description:
                column_names = [column[0] for column in cursor.description]
                data = cursor.fetchall()
                return column_names, data
        return None,None
    except Exception as e:
        print(e)
        await bridge.message_to_emacs("Sql Error, Please check python log.")
        await run_and_log("(websocket-bridge-app-open-buffer 'database)")
        return None,None

async def run_and_log(cmd):
    '''eval in emacs and log the command.'''
    print(cmd, flush=True)
    await bridge.eval_in_emacs(cmd)

async def new_database(db_name:str, info:DatabaseConnectInfo):
    "new a database connect"
    try:
        connections[db_name] = info.connect()
        database_configs[db_name] = info
        save_database_config(database_configs)
    except Exception as e:
        print(e)
        await bridge.message_to_emacs("database connect error")

def save_database_config(configs:dict):
    '''save database config to json file'''
    with open(db_config_file_path, "w", encoding="utf-8") as file:
        json.dump(configs, file)

async def init():
    '''init'''
    global database_configs, connections, db_config_file_path
    user_data_directory = await bridge.get_emacs_var("websocket-bridge-database-user-data-directory")
    user_data_directory = user_data_directory.strip('"')
    user_data_directory = os.path.expanduser(user_data_directory)
    db_config_file_path = os.path.join(user_data_directory, "database.json")
    create_user_data_file_if_not_exist(db_config_file_path, "{}")
    with open(db_config_file_path, "r", encoding="utf-8") as file:
        configs = json.load(file)
        print(dumps(configs))
        await bridge.eval_in_emacs(f'''(setq websocket-bridge-database-db-metas
        (ht<-plist '{dumps(configs).replace(':', '')}))''')
        for key, value in configs.items():
            db_type = value.get("db_type")
            host = value.get("host")
            port = value.get("port")
            user = value.get("user")
            password = value.get("password")
            info = DatabaseConnectInfo(db_type=db_type, host=host, port=port, user=user,password=password)
            try:
                info.connect()
                database_configs[key]=info
            except Exception as e:
                print(e)

def create_user_data_file_if_not_exist(path: str, content=None):
    '''create user data file if not exist'''
    if not os.path.exists(path):
        # Build parent directories when file is not exist.
        basedir = os.path.dirname(path)
        if not os.path.exists(basedir):
            os.makedirs(basedir)

        with open(path, "w", encoding="utf-8") as file:
            if content:
                file.write(content)

        print(f"[dictionary-overlay] auto create user data file {path}")

async def main():
    "main function"
    await asyncio.gather(init(), bridge.start())

asyncio.run(main())
