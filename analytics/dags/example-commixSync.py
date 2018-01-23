import datetime as dt
from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import HttpSensor
from airflow.models import Variable
import json

# refresh every 30 minutes
args = {
    'owner': 'commix',
    'depends_on_past': False,
    'start_date': dt.datetime(2017, 12, 7),
    'retries': 1,
    'email': ['tyler.allbritton@gettectonic.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retry_delay': dt.timedelta(minutes=5)
}

dag = DAG('commix-sync', default_args=args, schedule_interval='*/30 00,01,02,03,13,14,15,16,17,18,19,20,21,22,23 * * *', catchup=False)

def setStatusEndPoint(ds, **kwargs):
    """
    Build the name of the endpoint and set the Airflow shared "statusEndpoint" variable
    """
    Variable.set('sync_op-statusEndpoint', '/Sync/'+json.loads(kwargs['ti'].xcom_pull(task_ids='sync_op'))['job-id'])

def responseCheck(response):
    """
    Evaluate the response from the job status call. done or not.
    """
    if json.loads(response.content)['job-status'] == 'COMPLETE':
        return True
    if json.loads(response.content)['job-status'] == 'INPROGRESS':
        return False
    if json.loads(response.content)['job-status'] == 'NEW':
        raise ValueError('Sync Job does not exist')
    else:
        raise ValueError('Sync Job has failed')

# Make the asynchronous call to the i2ap data job
t1 = SimpleHttpOperator(
    task_id='sync_op',
    method='POST',
    xcom_push=True,
    http_conn_id='i2ap_processor',
    endpoint='/Sync',
    headers={"Content-Type": "application/json",
             "Tt-I2ap-Id": "i2ap-service@tt-cust-analytics.iam.gserviceaccount.com",
             "Tt-I2ap-Sec": "E8OLhEWWihzdpIz5"},
    dag=dag)

# retrieve the job id associated with the async call in t1
t2 = PythonOperator(
    task_id='sync_op_jobid',
    python_callable=setStatusEndPoint,
    provide_context=True,
    dag=dag
)

# loop on the status pull until the status is ERROR or COMPLETE
t3 = HttpSensor(
    task_id='sync_op_status',
    http_conn_id='i2ap_processor',
    endpoint=Variable.get('sync_op-statusEndpoint'),
    headers={"Content-Type": "application/json",
             "Tt-I2ap-Id": "i2ap-service@tt-cust-analytics.iam.gserviceaccount.com",
             "Tt-I2ap-Sec": "E8OLhEWWihzdpIz5"},
    response_check=responseCheck,
    poke_interval=60,
    dag=dag)

t2.set_upstream(t1)
t3.set_upstream(t2)
