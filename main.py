from fastapi import FastAPI,File, UploadFile
from typing import Optional
import csv 
from datetime import datetime
import codecs
import pandas as pd 
from schemas.avro import AvroSchemas
from schemas.pandas_schemas import PandasSchemas as ps
from schemas.headers import Headers as hd
from io import StringIO
import sqlite3
from fastavro import writer, parse_schema, reader
import os.path
from os import path
from messages.strings_msg import Messages

database = "database.sqlite"

app = FastAPI ()

@app.get("/")
def index():
    return {"Hello":"World"}

def processInformation(pd_schema:any,headers:list[str],tableName:str,file: UploadFile= File(...)):
    new_df=createDf(headers,file)
    errors =  ps.validateSchemas(pd_schema,new_df)
    if (len(errors)==0 and (not new_df.empty)):
        message= saveDf(new_df,tableName)
        return message
    return  Messages.file_wrong, errors

def createDf(columns:list[str],file:UploadFile= File(...)) :
    contents = file.file.read()
    s = str(contents,'utf-8')
    data = StringIO(s)
    try:
        df = pd.read_csv(data,header=None).fillna("NULL")
    except (pd.errors.ParserError, pd.errors.EmptyDataError):
        df = pd.DataFrame()
    if len(df.columns)==len(columns) and len(df)<1000:
        df.columns=columns
    else:
        df = pd.DataFrame()
    print(df)
    return df

def saveDf(df:pd.DataFrame,tableName:str):  
        try:
            conn = sqlite3.connect(database)
            df.to_sql(tableName,conn,if_exists='replace',index=False,dtype={'id': 'INTEGER PRIMARY KEY'})
            conn.close()
            return Messages.data_saved
        except sqlite3.IntegrityError:
             print(df)
             return Messages.duplicated_pk
        
@app.get("/restore/{nameTable}")       
def restoreBackup(nameTable):
    file_path='avros/'+nameTable+'.avro'
    avro_records = []
    if (path.isfile(file_path)):
        with open(file_path, 'rb') as fo:
            avro_reader = reader(fo)
            for record in avro_reader:
                avro_records.append(record)
        df=pd.DataFrame(avro_records)
        conn = sqlite3.connect(database)
        df.to_sql(nameTable,conn,if_exists='replace',index=False,dtype={'id': 'INTEGER PRIMARY KEY'})
        conn.close()
        return Messages.backup_ok
    else:
        return Messages.no_backup+nameTable
    
@app.get("/backup/{nameTable}")      
def createBackup(nameTable):
    conn = sqlite3.connect(database)
    try:
        df = pd.read_sql_query("SELECT * from "+nameTable, conn) 
        if(not df.empty):
            parsed_schema = parse_schema(AvroSchemas.getSchema(nameTable))
            records = df.to_dict('records')
            with open('avros/'+nameTable+'.avro', 'wb') as out:
                writer(out, parsed_schema, records)
            return Messages.data_saved
        else:
            return Messages.no_data+nameTable
    except pd.errors.DatabaseError:
         return Messages.no_data+nameTable
