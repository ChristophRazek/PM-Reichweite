import pandas as pd
import pyodbc
import SQL as s
import warnings

warnings.simplefilter("ignore")
conn_string = r'DRIVER={SQL Server}; server=172.19.128.2\emeadb; database=emea_enventa_live; UID=usr_razek; PWD=wB382^%H3INJ'
conx = pyodbc.connect(conn_string)

df =pd.read_sql(s.liste, conx)