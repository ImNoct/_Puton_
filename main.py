# import io
# import pandas as pd
# from unrar import rarfile
# import requests
# import dbf
#
# url = 'https://www.cbr.ru/vfs/credit/forms/101-20190101.rar'
#
# #response = requests.get(url)
#
# #rarred = rarfile.RarFile('101-20190101.rar')
# #rarred = rarfile.RarFile(io.BytesIO(response.content))
# #rfile = rarred.open(rarred.infolist()[1]) #0 - это индекс имени дескриптора первого
# #файла в списке. Не забывайте об этом, особенно, если у вас в архиве несколько файлов.
# df = dbf.Table('012020B1.DBF')
# df.open()
# print(df[10])
# df = dbf.Table('012020N1.DBF')
# df.open()
# print(df[11])
# df = dbf.Table('NAMES.DBF')
# df.open()
# print(df[12])
import time

import dbf
import mysql.connector

# source_path = r"\\path\to\file"
# file_name = "\\012020B1.DBF"

print("Found Source DBF")

source = dbf.Table("012020B1.DBF", codepage="cp866")
source.open()

print("Opened DBF")

db = mysql.connector.connect( host='localhost',
                                database='mybase',
                                user='root',
                                password='1234')
cur = db.cursor()
print("Connected to database")

try:
        cur.execute("DROP TABLE IF EXISTS table123")
except:
        db.rollback()

print("Dropped old table")

sql = """
        CREATE TABLE table123(    
                REGN      INTEGER,        
                PLAN      CHAR            default '',
                NUM_SC    CHAR(5)         default '',
                A_P       CHAR            default '',
                VR        BIGINT(16)     default 0,
                VV        BIGINT(16)     default 0,
                VITG      DECIMAL(33,4)   DEFAULT 0,
                ORA       BIGINT(16)     DEFAULT 0,
                OVA       BIGINT(16)     DEFAULT 0,
                OITGA     DECIMAL(33,4)   DEFAULT 0,
                ORP       BIGINT(16)     DEFAULT 0,
                OVP       BIGINT(16)     DEFAULT 0,
                OITGP     DECIMAL(33,4)   DEFAULT 0,
                IR        BIGINT(16)     DEFAULT 0,
                IV        BIGINT(16)     DEFAULT 0,
                IITG      DECIMAL(33,4)   DEFAULT 0,
                DT        DATE,          
                PRIZ      INTEGER(1)    DEFAULT 0,
                primary key (REGN, PLAN, NUM_SC, A_P)
        )"""

cur.execute(sql)

print("Created new table")
for r in source:
    query = """INSERT table123 SET
                REGN = %s, 
                PLAN = %s, 
                NUM_SC = %s, 
                A_P = %s, 
                VR = %s, 
                VV = %s, 
                VITG = %s, 
                ORA = %s,
                OVA = %s,
                OITGA = %s,
                ORP = %s,
                OVP = %s,
                OITGP = %s,
                IR = %s,
                IV = %s,
                IITG  = %s,
                DT = %s,
                PRIZ = %s"""
    values = (r["REGN"], r["PLAN"],
              r["NUM_SC"], r["A_P"],
              r["VR"], r["VV"],
              r["VITG"], r["ORA"],
              r["OVA"], r["OITGA"],
              r["ORP"], r["OVP"],
              r["OITGP"], r["IR"],
              r["IV"], r["IITG"],
              r["DT"], r["PRIZ"])
    cur.execute(query, values)
    db.commit()
    print(r["REGN"], r["PLAN"],
              r["NUM_SC"], r["A_P"],
              r["VR"], r["VV"],
              r["VITG"], r["ORA"],
              r["OVA"], r["OITGA"],
              r["ORP"], r["OVP"],
              r["OITGP"], r["IR"],
              r["IV"], r["IITG"],
              r["DT"], r["PRIZ"])

