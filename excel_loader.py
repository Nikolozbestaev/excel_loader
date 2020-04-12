import pandas as pd
import numpy as np
import psycopg2
from io import StringIO
#import os

# Для использования:
'''
import  excel_loader as el
file_nm = 'googleplaystore.csv'
dsn = 'dbname=... host=... port=... user=... password=...'
table_nm = 'sandpit.bestaev_tm'
el.run(file_nm, dsn, table_nm)
'''

# Функции
# Чтение файла
def read_file(file_nm):
    if file_nm.endswith('.csv'):
        try:
            df = pd.read_csv(file_nm, sep=';')
        except:
            df = pd.read_csv(file_nm)
    else:
        df = pd.read_excel(file_nm)
    print('Read file... Done')
    return df

# Чистка df
def clear_df(df):
    # Подготовка данных к записи в БД
    df.columns = [c.replace(' ','_').lower() for c in df.columns]
    for c in df.columns:
        df[c] = df[c].astype(str).str.replace('\n', '\\n').str.replace('|', '/')
    #df = df.where(pd.notnull(df), r'\N')
    print('Clear df... Done')
    return df

# Генерация табличных типов по типам df (dftypes)
def gen_tb_types(df):
    type_conv = {np.dtype('int64'):'bigint'
                    ,np.dtype('O'): 'text'
                    ,np.dtype('float64'):'real'
                    ,np.dtype('<M8[ns]'):'date'}
    col_types = {}
    for col in df.columns:
        try:
            col_types[col] = type_conv[df[col].dtypes]
        except:
            col_types[col] = 'text'
    print('Generate database types... Done')
    return col_types

# Подключение к БД
def connto(dsn):
    #dsn = os.environ.get('DB_DSN')  # Use ENV vars: keep it secret, keep it safe
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    print('Connect to database... Done')
    return conn

# Создание таблицы в БД
def create_tb(conn, table_name, col_types):
    table_col = ', '.join(['"'+a+'"'+' '+b for a,b in list(zip(list(col_types.keys()), list(col_types.values())))])
    with conn.cursor() as cursor:
        cursor.execute("""DROP TABLE IF EXISTS %s;CREATE UNLOGGED TABLE %s (%s);"""%(table_name, table_name, table_col))
    print('Create database table... Done')
    return

# Запись таблицы
def load_df(conn, df, table_name):
    # Initialize a string buffer
    sio = StringIO()
    sio.write(df.to_csv(index=None, header=None, sep='|'))  # Write the Pandas DataFrame as a csv to the buffer
    sio.seek(0)  # Be sure to reset the position to the start of the stream

    # Copy the string buffer to the database, as if it were an actual file
    with conn.cursor() as c:
        c.execute('TRUNCATE TABLE %s;'%(table_name))
        c.copy_from(sio, table_name, columns=df.columns, sep='|')
    print('Load data to database... Done')
    return

# Запуск модуля
def run(file_nm, dsn, table_nm):
    # Чтение файла:
    df = read_file(file_nm)
    # Чистка df:
    df = clear_df(df)
    # Генерация табличных типов по типам df (dftypes):
    col_types = gen_tb_types(df)
    # Установить соединение к БД:
    conn = connto(dsn)
    # Создание таблицы в БД:
    create_tb(conn, table_nm, col_types)
    # Запись таблицы
    load_df(conn, df, table_nm)
    # Закрыть соединение к БД
    conn.close()
    print('Close connection... Done')
    return
