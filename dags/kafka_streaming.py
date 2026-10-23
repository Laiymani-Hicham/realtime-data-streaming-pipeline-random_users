
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
from kafka import KafkaProducer
import time
import logging
import uuid 

default_args = {
    'owner':'airflow',
    'start_date':datetime(2026,10,13,9,13)
}

def get_data():
    request = requests.get('https://randomuser.me/api/')
    data = request.json()
    user = data['results'][0]
    return user

def format_data(user):
    data = {}
    location = user['location']
    data['id'] = str(uuid.uuid4())
    data ['first_name'] = user['name']['first']
    data ['last_name'] = user['name']['last']
    data ['gender'] = user['gender']
    data['address'] = f"{location['street']['number']} {location['street']['name']} {location['city']} {location['state']} {location['country']}"
    data ['post_code'] = location['postcode']
    data['email'] = user['email']
    data ['username'] = user['login']['username']
    data ['registered_date'] = user['registered']['date']
    data ['phone'] = user ['phone']
    data ['picture'] = user['picture']['medium']
    return data

def streaming_data():
    producer = KafkaProducer(bootstrap_servers=['broker:29092']) #we create the producer and we point it to kafka
    now = time.time()
    while True:
        if time.time() > now + 60:
            break
        try:
            res = get_data()
            clean_data = format_data(res)
            print(json.dumps(clean_data , indent=3))
            producer.send('users_created',json.dumps(clean_data).encode('utf-8'))
        except Exception as error: #Exeception is the wild card for all erros in python
            logging.error(f'Error: {error}')
            continue
        

with DAG(
    'user_automation',
    default_args = default_args , 
    schedule_interval = '@daily',
    catchup = False
) as dag:
    streaming_task = PythonOperator(task_id = 'streaming_data_from_api' , python_callable = streaming_data)

 


