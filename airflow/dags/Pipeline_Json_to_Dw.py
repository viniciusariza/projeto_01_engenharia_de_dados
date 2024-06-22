import datetime as dt
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from scripts import Json_to_Stage, Stage_to_Dw

# Argumentos
default_args ={
    'owner':'Vinicius Ariza',
    'depends_on_past':False,
    'start_date':dt.datetime.today(),
    'retries':0,
}

# DAG
dag = DAG('Capim_Json_to_Dw',
           default_args = default_args,
           schedule_interval = '0 8 * * *',
)

json_to_stage = PythonOperator(task_id = 'Json_to_Stage',
                             python_callable = Json_to_Stage.main,
                             dag = dag
)

stage_to_dw = PythonOperator(task_id = 'Stage_to_Dw',
                             python_callable = Stage_to_Dw.main,
                             dag = dag
)

json_to_stage >> stage_to_dw