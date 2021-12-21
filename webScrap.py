from urllib.request import urlopen
from datetime import datetime, timedelta
import re
from airflow.models.dag import ScheduleInterval
import requests
import logging

import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

start_date_dag = airflow.utils.dates.days_ago(0)

MAX_TIMEOUT = timedelta(minutes=10)
important = []
keep_phrases = ["Finalizando importacao lote"]
url =r'http://10.85.20.8/maj/rsfilemanager.log'

def search_file():
    try:
      file = urlopen(url)
    except requests.Timeout as err:
      logging.error("Tempo excedido", err)
    return file

file = search_file()

def scrap_file(important, keep_phrases, file):
    for line in file:
      for phrase in keep_phrases:
        if phrase in line.decode("utf-8"):
          important.append(line.decode("utf-8"))
          break

    log_text = important[len(important)-1]
    matches = re.findall(r'(\d+/\d+/\d+ \d+:\d+:\d+)',log_text)

    date_time_str = matches[0]
    date_time_obj = datetime.strptime(date_time_str, '%Y/%m/%d %H:%M:%S')

    date_time_difference =  datetime.today() - date_time_obj
    return log_text,date_time_str,date_time_difference

log_text, date_time_str, date_time_difference = scrap_file(important, keep_phrases, file)

def show_results(MAX_TIMEOUT, log_text, date_time_str, date_time_difference):
    print('Data atual: ', datetime.today() )
    print('Dia do ultimo log: ',date_time_str)
    print('Tempo entre o ultimo log e a data atual: ', date_time_difference)
    print(log_text)
    print('\n')
    if (date_time_difference >= MAX_TIMEOUT):
      print("Erro, tempo entre ultimo log e data atual maior que: ", MAX_TIMEOUT)

show_results(MAX_TIMEOUT, log_text, date_time_str, date_time_difference)


def execute_dag():
  file = search_file()
  log_text, date_time_str, date_time_difference = scrap_file(important, keep_phrases, file)
  show_results(MAX_TIMEOUT, log_text, date_time_str, date_time_difference)


args = {
    'depends_on_past': False,
    'start_date': start_date_dag
}

dag = DAG(
    dag_id='verificacao_log',
    default_args=args,
    tags=['verificação'],
    schedule_interval = timedelta(minutes=10)
)

task = PythonOperator(
    task_id='verificacao_log',
    python_callable=execute_dag,
    retries=0,
    dag=dag
)
