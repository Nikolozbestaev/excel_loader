# excel_loader
Позволяет  загрузить данне Excel файла в базу данных.
Пример использования: 

import  excel_loader as el

file_nm = r'test.xlsx'

dsn = 'dbname=... host=... port=... user=... password=...'

table_nm = 'schema_test.table_test'

el.run(file_nm, dsn, table_nm)
