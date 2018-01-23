import cx_Oracle
import pandas as pd

ip = '172.18.0.1'
port = 1521
SID = 'RHTestDB'
dsn_tns = cx_Oracle.makedsn(ip, port, SID)
db = cx_Oracle.connect('readonlyuser', 'MyPassword', dsn_tns)

df_ora = pd.read_sql('SELECT * FROM PEOPLE', con=db)
df_ora
