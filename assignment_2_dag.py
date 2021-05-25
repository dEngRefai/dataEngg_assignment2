import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator 
import pandas as pd
import psycopg2
import json
from sqlalchemy import create_engine

from random import randint  ## check if needed in the end
import subprocess

default_args={
    'owner': 'refai',
    'start_date': dt.datetime(2021, 5, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


    
def _install_tools():
    try:
        from faker import Faker
    except:
        subprocess.check_call(['pip' ,'install', 'faker' ])
        from faker import Faker
        
    ### check if all these library will be needed     
    try:
        import psycopg2 
    except:
        subprocess.check_call(['pip' ,'install', 'psycopg2-binary' ])
        import psycopg2
        
    try:
        from sqlalchemy import create_engine
    except:
        subprocess.check_call(['pip' ,'install', 'sqlalchemy' ])
        from sqlalchemy import create_engine
        
        
    try:
        import pandas as pd 
    except:
        subprocess.check_call(['pip' ,'install', 'pandas' ])
        import pandas as pd 
        
    try:
        import pymongo 
    except:
        subprocess.check_call(['pip' ,'install', 'pymongo' ])
        import pymongo 
        

        

        
def queryPostgresql():
    # define engine
    host="postgres_storage"
    database="testDB"
    user="me"
    password="1234"
    port='5432'
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
    
    #pull table into dataframe
    DFpostgress=pd.read_sql("SELECT * FROM users2020" , engine);
    
    #print to the logs the pulled table
    print(DFpostgress.head(10))
    
    #transfor the datafram extracted form postgress to csv file
    DFpostgress.to_csv('/opt/airflow/dags/frompostgresss.csv') #check mounting of volumes       
        

    #print the name of tables in data base 
    print(engine.table_names()) # check befor puplish
        

def CSVToJson():
    
    #csv to json
    df=pd.read_csv('/opt/airflow/dags/frompostgresss.csv') 
    df.to_json('/opt/airflow/dags/datatojson.json', orient='split', index=False)
    
    #print to the logs
    for i,r in df.iterrows():
        print(r['name'])

def insertMongoDB():
    import json
    from pymongo import MongoClient
    
    #connect to MongoDB
    client = MongoClient('mongo:27017', 
                        username='root',
                         password='example')
    #pushdata
    db = client['faker_db']
    collection_fake = db['fakefake']

    with open('/opt/airflow/dags/datatojson.json') as f:
        file_data = json.load(f)
        collection_fake.insert_one(file_data)
  

with DAG('postgressCSVJSONmongodbDAG',
        default_args=default_args,
        schedule_interval=timedelta(minutes=1),
        catchup=False
        ) as dag:


    install_tools = PythonOperator(task_id="install_tools",
                                    python_callable=_install_tools)
    getDATA = PythonOperator(task_id='QueryPostgreSQL', 
                             python_callable=queryPostgresql)
    CSVJson = PythonOperator(task_id='convertCSVtoJson', 
                             python_callable=CSVToJson)
    insertData = PythonOperator(task_id='InsertDataMongoDB', 
                             python_callable=insertMongoDB)

install_tools >> getDATA >> CSVJson >>  insertData
