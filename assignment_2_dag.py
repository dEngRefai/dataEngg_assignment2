import datetime as dt
from datetime import timedelta
from airflow import DAG

from airflow.operators.bash_operator import BashOperator

from airflow.operators.python_operator import PythonOperator 
import pandas as pd
import psycopg2
import json
from sqlalchemy import create_engine

## check if needed for all 
from random import randint  ## check if needed in the end


import subprocess

default_args={
    'owner': 'refai',
    'start_date': dt.datetime(2021, 5, 26),
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
        import matplotlib 
    except:
        subprocess.check_call(['pip' ,'install', 'matplotlib' ])
        import matplotlib

    try:
        import sklearn 
    except:
        subprocess.check_call(['pip' ,'install', 'sklearn' ])
        import sklearn
    

    

    
def extractfromGithub():
    List_of_days=[]
    for year in range(2020,2022):
      for month in range(1,13):
        for day in range(1,32):
          month=int(month)
          if day <=9:
            day=f'0{day}'

          if month <= 9 :
            month=f'0{month}'
          List_of_days.append(f'{month}-{day}-{year}')
    
    Day='01-01-2021' #could be updated 
    import pandas as pd 
    def Get_DF_i(Day):   
        DF_i=None
        try: 
            URL_Day=f'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/{Day}.csv'
            DF_day=pd.read_csv(URL_Day)
            DF_day['Day']=Day
            cond=(DF_day.Country_Region=='Jordan')
            Selec_columns=['Day','Country_Region', 'Last_Update',
              'Lat', 'Long_', 'Confirmed', 'Deaths', 'Recovered', 'Active',
              'Combined_Key', 'Incident_Rate', 'Case_Fatality_Ratio']
            DF_i=DF_day[cond][Selec_columns].reset_index(drop=True) 
    
        except:
            pass
        return DF_i
    
    DF_all=[]
    for Day in List_of_days:
        DF_all.append(Get_DF_i(Day))
        
    DF_Jordan=pd.concat(DF_all).reset_index(drop=True)
    
    # Create DateTime for Last_Update
    DF_Jordan['Last_Updat']=pd.to_datetime(DF_Jordan.Last_Update, infer_datetime_format=True)  
    DF_Jordan['Day']=pd.to_datetime(DF_Jordan.Day, infer_datetime_format=True)  

    DF_Jordan['Case_Fatality_Ratio']=DF_Jordan['Case_Fatality_Ratio'].astype(float)
        
    
    
    
    # define engine
    host="postgres_storage"
    database="testDB"
    user="me"
    password="1234"
    port='5432'
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
    
    #insert DF_Jordan into postgres
    
    DF_Jordan.to_sql('DF_jordan', engine, if_exists='replace', index=False)
    
    
    
    
    
    
    
    
    DFpostgress=pd.read_sql("SELECT * FROM users2020" , engine);
    
    
    
    
        

  

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


install_tools >> extractfromGithub

 
